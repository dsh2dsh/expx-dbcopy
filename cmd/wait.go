package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	errorExt   = ".error"
	okExt      = ".ok"
	sqlExt     = ".bz2.crypt"
	startedExt = ".started"
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
			return NewWait(s3Client, s3Bucket).WithTimeout(waitMax).
				Run(context.Background(), args[0])
		},
	}

	waitMax time.Duration
)

func init() {
	waitCmd.Flags().DurationVarP(&waitMax, "timeout", "t", time.Hour,
		"wait timeout")
}

func NewWait(client *s3.Client, bucket string) *Wait {
	return &Wait{
		client:  client,
		bucket:  bucket,
		waitMax: waitMax,
		barC:    make(chan func()),
	}
}

type Wait struct {
	client  *s3.Client
	bucket  string
	waitMax time.Duration
	barC    chan func()
}

func (self *Wait) WithTimeout(d time.Duration) *Wait {
	self.waitMax = d
	return self
}

func (self *Wait) Run(ctx context.Context, object string) error {
	ctx2, completed := context.WithCancel(ctx)
	defer completed()

	g, groupCtx := errgroup.WithContext(ctx2)
	g.Go(func() error { return self.renderProgress(groupCtx) })
	self.withProgress(func() { log.Println("waiting for", object+sqlExt) })

	g.Go(func() error { return self.waitStarted(groupCtx, object) })
	g.Go(func() error { return self.waitError(groupCtx, object) })
	g.Go(func() error { return self.waitOk(groupCtx, object, completed) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("wait for %q: %w", object, err)
	}
	return self.printSize(ctx, object+sqlExt)
}

func (self *Wait) renderProgress(ctx context.Context) error {
	bar := progressbar.NewOptions64(-1,
		progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }),
		progressbar.OptionSetDescription("waiting"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionUseANSICodes(true),
	)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := bar.Clear(); err != nil {
				return fmt.Errorf("progress clear: %w", err)
			}
			return nil
		case <-ticker.C:
			if err := bar.Add64(1); err != nil {
				return fmt.Errorf("progress add: %w", err)
			}
		case fn := <-self.barC:
			if err := clearBar(bar, fn); err != nil {
				return err
			}
		}
	}
}

func clearBar(bar *progressbar.ProgressBar, fn func()) error {
	if err := bar.Clear(); err != nil {
		return fmt.Errorf("progress clear: %w", err)
	}
	fn()
	if err := bar.RenderBlank(); err != nil {
		return fmt.Errorf("progress render: %w", err)
	}
	return nil
}

func (self *Wait) waitStarted(ctx context.Context, prefix string) error {
	key := prefix + startedExt
	if err := self.waitObject(ctx, key); err != nil {
		return err
	}
	self.withProgress(func() { log.Println("got", startedExt) })
	return nil
}

func (self *Wait) waitObject(ctx context.Context, key string,
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

func (self *Wait) withProgress(fn func()) { self.barC <- fn }

func (self *Wait) waitError(ctx context.Context, prefix string) error {
	key := prefix + errorExt
	if err := self.waitObject(ctx, key); err != nil {
		return err
	}
	self.withProgress(func() { log.Println("got", errorExt) })
	return fmt.Errorf("remote error:\n%w", self.readError(ctx, key))
}

func (self *Wait) readError(ctx context.Context, key string) error {
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

func (self *Wait) waitOk(ctx context.Context, prefix string, completed func(),
) error {
	key := prefix + okExt
	if err := self.waitObject(ctx, key); err != nil {
		return err
	}
	self.withProgress(func() { log.Println("got", okExt) })
	completed()
	return nil
}

func (self *Wait) printSize(ctx context.Context, key string) error {
	resp, err := self.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("heading %q: %w", key, err)
	}

	contentLength := aws.ToInt64(resp.ContentLength)
	humanSize, sizeSuffix := humanizeBytes(contentLength, true)
	log.Println("size:", humanSize, sizeSuffix)
	fmt.Println(contentLength)
	return nil
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
