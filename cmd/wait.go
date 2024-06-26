package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

const (
	errorExt   = ".error"
	okExt      = ".ok"
	sqlExt     = ".bz2.crypt"
	startedExt = ".started"

	barPad      = 8
	barMaxWidth = 64
)

var (
	waitCmd = cobra.Command{
		Use:                   "wait -b my-bucket [-t timeout] name",
		Short:                 "Wait for name.bz2.crypt",
		Args:                  cobra.ExactArgs(1),
		DisableFlagsInUseLine: true,

		RunE: func(cmd *cobra.Command, args []string) error {
			if err := rootSetup(); err != nil {
				return err
			}
			return Wait(args[0])
		},
	}

	waitMax time.Duration

	green      = lipgloss.NewStyle().Foreground(lipgloss.Color("2")).Render
	helpStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262")).Render
	sinceStyle = lipgloss.NewStyle().Width(barPad).Padding(0, 1).
			AlignHorizontal(lipgloss.Right).Render
)

func init() {
	waitCmd.Flags().DurationVarP(&waitMax, "timeout", "t", 30*time.Minute,
		"wait timeout")
}

func Wait(object string) error {
	model := NewWaitModel(s3Client, s3Bucket, object).WithTimeout(waitMax)
	defer model.Wait()
	progress := tea.NewProgram(model, tea.WithOutput(os.Stderr))

	if _, err := progress.Run(); err != nil {
		return fmt.Errorf("tea program: %w", err)
	}

	err := context.Cause(model.running)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("canceled: %w", err)
	}

	fmt.Println(model.contentLength)
	return nil
}

func NewWaitModel(client *s3.Client, bucket string, object string) *WaitModel {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &WaitModel{
		client: client,
		bucket: bucket,
		object: object,

		running: ctx,
		cancel:  cancel,

		progress: progress.New(progress.WithoutPercentage(),
			progress.WithDefaultGradient()),
	}
}

type WaitModel struct {
	client  *s3.Client
	bucket  string
	object  string
	waitMax time.Duration

	wg        sync.WaitGroup
	startedAt time.Time
	b         strings.Builder

	percent  float64
	progress progress.Model

	running context.Context
	cancel  context.CancelCauseFunc

	contentLength int64
}

type (
	waitMsg struct {
		started bool
		size    int64
		err     error
	}
)

func tickCmd(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

type tickMsg time.Time

func (self *WaitModel) WithTimeout(d time.Duration) *WaitModel {
	self.waitMax = d
	return self
}

func (self *WaitModel) Wait() {
	self.cancel(nil)
	self.wg.Wait()
}

func (self *WaitModel) Init() tea.Cmd {
	self.startedAt = time.Now()
	return tea.Sequence(
		tea.Println("waiting for ", self.object+sqlExt),
		tickCmd(time.Second),
		tea.Batch(self.waitStarted(), self.waitError(), self.waitOk()))
}

func (self *WaitModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return self.handleKeys(msg)
	case waitMsg:
		return self.handleWaits(msg)
	case tea.WindowSizeMsg:
		self.progress.Width = msg.Width - barPad*2
		if self.progress.Width > barMaxWidth {
			self.progress.Width = barMaxWidth
		}
	case tickMsg:
		self.percent = min(1.0,
			time.Since(self.startedAt).Seconds()/self.waitMax.Seconds())
		return self, tickCmd(time.Second)
	}
	return self, nil
}

func (self *WaitModel) handleKeys(m tea.KeyMsg) (*WaitModel, tea.Cmd) {
	switch m.String() {
	case "ctrl+c", "q", "esc":
		self.cancel(fmt.Errorf("by user input: %s", m.String()))
		return self, self.quitCmd
	}
	return self, nil
}

func (self *WaitModel) quitCmd() tea.Msg {
	self.cancel(nil)
	return tea.Quit()
}

func (self *WaitModel) handleWaits(m waitMsg) (*WaitModel, tea.Cmd) {
	if m.err != nil {
		self.cancel(m.err)
		return self, self.quitCmd
	} else if m.started {
		return self, tea.Sequence(tea.Println(green("✓ started"),
			" [", time.Since(self.startedAt).Truncate(time.Second), "]"))
	}

	self.contentLength = m.size
	humanSize, sizeSuffix := humanizeBytes(m.size, true)

	return self, tea.Sequence(
		tea.Println(green("✓ ok:"),
			" ", humanSize, " ", sizeSuffix,
			" [", time.Since(self.startedAt).Truncate(time.Second), "]"),
		self.quitCmd)
}

func (self *WaitModel) View() string {
	if self.running.Err() != nil {
		return ""
	}

	b := self.b
	b.Reset()
	b.WriteString("\n")

	d := time.Since(self.startedAt)
	b.WriteString(sinceStyle(d.Truncate(time.Second).String()))

	b.WriteString(self.progress.ViewAs(self.percent))

	b.WriteString(" ")
	timeLeft := self.waitMax - d
	b.WriteString(timeLeft.Truncate(time.Second).String())

	b.WriteString("\n\n")
	b.WriteString(helpStyle("Press Esc/C-c/q to quit"))

	return b.String()
}

func (self *WaitModel) waitStarted() tea.Cmd {
	self.wg.Add(1)
	return func() tea.Msg {
		defer self.wg.Done()
		err := self.waitObject(self.running, self.object+startedExt)
		if err != nil {
			return waitMsg{err: err}
		}
		return waitMsg{started: true}
	}
}

func (self *WaitModel) waitObject(ctx context.Context, key string,
	callbacks ...func(headObject *s3.HeadObjectOutput),
) error {
	h, err := s3.NewObjectExistsWaiter(self.client).WaitForOutput(
		ctx, &s3.HeadObjectInput{
			Bucket: aws.String(self.bucket),
			Key:    aws.String(key),
		}, self.waitMax)
	if err != nil {
		return fmt.Errorf("wait for %q: %w", key, err)
	}

	for _, fn := range callbacks {
		fn(h)
	}
	return nil
}

func (self *WaitModel) waitError() tea.Cmd {
	self.wg.Add(1)
	return func() tea.Msg {
		defer self.wg.Done()
		key := self.object + errorExt
		if err := self.waitObject(self.running, key); err != nil {
			return waitMsg{err: err}
		}
		return waitMsg{
			err: fmt.Errorf("remote error:\n%w", self.readError(self.running, key)),
		}
	}
}

func (self *WaitModel) readError(ctx context.Context, key string) error {
	resp, err := self.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("reading %q: %w", key, err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading all from %q: %w", key, err)
	}
	return errors.New(string(b))
}

func (self *WaitModel) waitOk() tea.Cmd {
	self.wg.Add(1)
	return func() tea.Msg {
		defer self.wg.Done()
		if err := self.waitObject(self.running, self.object+okExt); err != nil {
			return waitMsg{err: err}
		}

		size, err := self.size(self.running, self.object+sqlExt)
		if err != nil {
			return waitMsg{err: err}
		}

		return waitMsg{size: size}
	}
}

func (self *WaitModel) size(ctx context.Context, key string) (int64, error) {
	resp, err := self.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("heading %q: %w", key, err)
	}
	return aws.ToInt64(resp.ContentLength), nil
}

func humanizeBytes(s int64, iec bool) (string, string) {
	sizes := [...]string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}
	base := 1000.0

	if iec {
		sizes = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
		base = 1024.0
	}

	if s < 10 {
		return fmt.Sprintf("%.0f", float64(s)), sizes[0]
	}

	e := math.Floor(math.Log(float64(s)) / math.Log(base))
	suffix := sizes[int(e)]
	v := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	if v < 10 {
		return fmt.Sprintf("%.1f", v), suffix
	}
	return fmt.Sprintf("%.0f", v), suffix
}
