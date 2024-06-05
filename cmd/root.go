package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/spf13/cobra"
)

var (
	rootCmd = cobra.Command{
		Use: "dbcopy",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Don't show usage on app errors.
			// https://github.com/spf13/cobra/issues/340#issuecomment-378726225
			cmd.SilenceUsage = true
		},
	}

	s3Client *s3.Client
	s3Bucket string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&s3Bucket, "bucket", "b", "",
		"S3 bucket")
	_ = rootCmd.MarkPersistentFlagRequired("bucket")

	rootCmd.AddCommand(&catCmd)
	rootCmd.AddCommand(&waitCmd)
}

func Execute(version string) {
	if version != "" {
		rootCmd.Version = version
	}
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func rootSetup() error {
	if err := loadEnvs(); err != nil {
		return err
	}

	if c, err := newS3Client(); err != nil {
		return err
	} else {
		s3Client = c
	}
	return nil
}

func loadEnvs() error {
	if err := dotenv.New().WithDepth(1).Load(); err != nil {
		return fmt.Errorf("load .env: %w", err)
	}
	return nil
}

func newS3Client() (*s3.Client, error) {
	ctx := context.Background()

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	region, err := manager.GetBucketRegion(ctx, s3.NewFromConfig(cfg), s3Bucket)
	if err != nil {
		return nil, fmt.Errorf("region of bucket %q: %w", s3Bucket, err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.Region = region })
	return client, nil
}
