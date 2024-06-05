package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

var catCmd = cobra.Command{
	Use:                   "cat -b my-bucket name",
	Short:                 "Output name.bz2.crypt to stdout",
	Args:                  cobra.ExactArgs(1),
	DisableFlagsInUseLine: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := rootSetup(); err != nil {
			return err
		}
		return NewCat(s3Client, s3Bucket).Run(context.Background(), args[0])
	},
}

func NewCat(client *s3.Client, bucket string) *Cat {
	return &Cat{client: client, bucket: bucket}
}

type Cat struct {
	client *s3.Client
	bucket string
}

func (self *Cat) Run(ctx context.Context, name string) error {
	key := name + sqlExt
	log.Println("download", key)
	resp, err := self.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("read %q: %w", key, err)
	}
	defer resp.Body.Close()

	if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
		return fmt.Errorf("copy %q to stdout: %w", key, err)
	}
	return nil
}
