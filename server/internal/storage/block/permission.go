package block

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/mageg-x/dedups3/internal/utils"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	xconf "github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
)

// TestS3AccessPermissions S3 storage permission test
func TestS3AccessPermissions(c *xconf.S3Config) error {
	logger.GetLogger("dedups3").Infof("Starting standalone S3 storage permission test: %s", c.Endpoint)

	if c == nil {
		return errors.New("nil config")
	}

	ctx := context.Background()
	client, err := createS3Client(c)
	if err != nil {
		return err
	}

	bucketName := c.Bucket
	blockID, testObjKey, testData := generateS3TestParams()
	clean := true
	// 执行权限测试
	if err := testS3WritePermission(ctx, client, bucketName, testObjKey, testData); err != nil {
		return err
	}
	defer func() {
		if clean {
			_ = testS3DeletePermission(ctx, client, bucketName, testObjKey)
		}
	}()
	if err := testS3ReadPermission(ctx, client, bucketName, testObjKey, testData); err != nil {
		return err
	}
	if err := testS3ListPermission(ctx, client, bucketName, testObjKey, blockID); err != nil {
		return err
	}
	if err := testS3DeletePermission(ctx, client, bucketName, testObjKey); err != nil {
		return err
	}
	clean = false

	logger.GetLogger("dedups3").Infof("All S3 storage permission tests passed")
	return nil
}

// TestDiskAccessPermissions Disk storage permission test
func TestDiskAccessPermissions(c *xconf.DiskConfig) error {
	logger.GetLogger("dedups3").Infof("Starting standalone disk storage permission test: %s", c.Path)
	if c == nil {
		return errors.New("nil config")
	}
	// 确保存储路径存在
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to create disk storage path: %v", err)
		return fmt.Errorf("Failed to create disk storage path: %w", err)
	}

	testDir, testFilePath, testData := generateDiskTestParams(c.Path)

	clean := true
	// 执行权限测试
	if err := testDiskWritePermission(testFilePath, testData); err != nil {
		return err
	}
	defer func() {
		if clean {
			_ = testDiskDeletePermission(testFilePath, c.Path)
		}
	}()
	if err := testDiskReadPermission(testFilePath, testData); err != nil {
		return err
	}
	if err := testDiskListPermission(testDir, testFilePath); err != nil {
		return err
	}
	if err := testDiskDeletePermission(testFilePath, c.Path); err != nil {
		return err
	}
	clean = false
	logger.GetLogger("dedups3").Infof("All disk storage permission tests passed")
	return nil
}

// getBlockPath Construct block path in S3, identical to s3.go
func getBlockPath(blockID string) string {
	n := len(blockID)
	// 确保blockID长度足够
	if n < 9 {
		// 如果blockID太短，使用一种简化的路径结构
		return path.Join("dedups3", "blocks", "short", blockID)
	}
	dir1 := blockID[n-3:]      // 最后3位
	dir2 := blockID[n-6 : n-3] // 倒数第4-6位
	dir3 := blockID[n-9 : n-6] // 倒数第7-9位
	return path.Join("dedups3", "blocks", dir1, dir2, dir3, blockID)
}

// createS3Client Create S3 client
func createS3Client(c *xconf.S3Config) (*s3.Client, error) {
	credentialProvider := credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")
	httpcli := &http.Client{
		Transport: &xhttp.HttpLoggingTransport{
			Transport: http.DefaultTransport,
		},
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentialProvider),
		config.WithHTTPClient(httpcli),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithLogger(logger.AWSNullLogger{}),
	)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to load SDK configuration: %v", err)
		return nil, fmt.Errorf("failed to load SDK configuration: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.Endpoint)
		o.UsePathStyle = c.UsePathStyle
		o.RetryMaxAttempts = 1
		logger.GetLogger("dedups3").Debugf("Use path style: %v", c.UsePathStyle)
	})

	return client, nil
}

// generateS3TestParams Generate S3 test parameters
func generateS3TestParams() (string, string, []byte) {
	blockID := utils.GenUUID()
	testObjKey := getBlockPath(blockID)
	testData := []byte("dedups3 storage test data for permission check - " + time.Now().Format(time.RFC3339Nano))
	return blockID, testObjKey, testData
}

// generateDiskTestParams Generate disk test parameters
func generateDiskTestParams(basePath string) (string, string, []byte) {
	testDir := filepath.Join(basePath, "dedups3-test", time.Now().Format("20060102150405"))
	testFileName := generateRandomString(8) + ".test"
	testFilePath := filepath.Join(testDir, testFileName)
	testData := []byte("dedups3 storage test data for permission check - " + time.Now().Format(time.RFC3339Nano))
	return testDir, testFilePath, testData
}

// testS3WritePermission Test S3 write permission
func testS3WritePermission(ctx context.Context, client *s3.Client, bucketName, testObjKey string, testData []byte) error {
	logger.GetLogger("dedups3").Infof("Starting S3 write permission test: %s/%s", bucketName, testObjKey)

	uploadInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testObjKey),
		Body:   strings.NewReader(string(testData)),
	}

	_, err := client.PutObject(ctx, uploadInput)
	if err != nil {
		return handleS3Error("S3 write permission test failed", err)
	}
	logger.GetLogger("dedups3").Infof("S3 write permission test passed")
	return nil
}

// testS3ReadPermission Test S3 read permission
func testS3ReadPermission(ctx context.Context, client *s3.Client, bucketName, testObjKey string, expectedData []byte) error {
	logger.GetLogger("dedups3").Infof("Starting S3 read permission test: %s/%s", bucketName, testObjKey)

	downloadInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testObjKey),
	}

	getResp, err := client.GetObject(ctx, downloadInput)
	if err != nil {
		return handleS3Error("S3 read permission test failed", err)
	}
	defer getResp.Body.Close()

	downloadedData, err := io.ReadAll(getResp.Body)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to read S3 object data: %v", err)
		return fmt.Errorf("Failed to read S3 object data: %w", err)
	}

	if string(downloadedData) != string(expectedData) {
		err = errors.New("s3 data verification failed: downloaded data does not match original data")
		logger.GetLogger("dedups3").Errorf("%v", err)
		return err
	}
	logger.GetLogger("dedups3").Infof("S3 read permission test passed")
	return nil
}

// testS3ListPermission Test S3 list permission
func testS3ListPermission(ctx context.Context, client *s3.Client, bucketName, testObjKey, blockID string) error {
	logger.GetLogger("dedups3").Infof("Starting S3 list permission test: %s", bucketName)

	// 创建多个测试块以验证列表功能
	additionalBlockIDs := []string{
		blockID + "-1",
		blockID + "-2",
		blockID + "-3",
	}

	for _, id := range additionalBlockIDs {
		additionalKey := getBlockPath(id)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(additionalKey),
			Body:   strings.NewReader("additional test data"),
		})
		if err != nil {
			logger.GetLogger("dedups3").Errorf("Failed to create additional test block(%s): %v", id, err)
		}
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("dedups3/blocks/"),
	}

	listResp, err := client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return handleS3Error("S3 list permission test failed", err)
	}

	// 验证测试对象在列表中
	found := false
	for _, obj := range listResp.Contents {
		if *obj.Key == testObjKey {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("s3 list permission test failed: test object not found in list")
	}
	logger.GetLogger("dedups3").Infof("S3 list permission test passed")

	// 清理额外创建的测试块
	for _, id := range additionalBlockIDs {
		additionalKey := getBlockPath(id)
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(additionalKey),
		})
	}
	return nil
}

// testS3DeletePermission Test S3 delete permission
func testS3DeletePermission(ctx context.Context, client *s3.Client, bucketName, testObjKey string) error {
	logger.GetLogger("dedups3").Infof("Starting S3 delete permission test: %s/%s", bucketName, testObjKey)

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testObjKey),
	}

	_, err := client.DeleteObject(ctx, deleteInput)
	if err != nil {
		return handleS3Error("S3 delete permission test failed", err)
	}

	// 验证对象已被删除
	downloadInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testObjKey),
	}
	_, err = client.GetObject(ctx, downloadInput)
	if err == nil {
		return fmt.Errorf("s3 delete permission test failed: object still readable after deletion")
	}

	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		// 确认是NoSuchKey错误，说明删除成功
	} else {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			return fmt.Errorf("s3 delete permission verification failed: expected NoSuchKey error, actual error code: %s", apiErr.ErrorCode())
		}
		return fmt.Errorf("s3 delete permission verification failed: error checking deletion result: %w", err)
	}
	logger.GetLogger("dedups3").Infof("S3 delete permission test passed")
	return nil
}

// handleS3Error Handle S3 errors
func handleS3Error(operation string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		logger.GetLogger("dedups3").Errorf("%s [%s]: %v", operation, apiErr.ErrorCode(), apiErr.ErrorMessage())
		return fmt.Errorf("%s [%s]: %s", operation, apiErr.ErrorCode(), apiErr.ErrorMessage())
	}
	logger.GetLogger("dedups3").Errorf("%s: %v", operation, err)
	return fmt.Errorf("%s: %w", operation, err)
}

// testDiskWritePermission Test disk write permission
func testDiskWritePermission(testFilePath string, testData []byte) error {
	logger.GetLogger("dedups3").Infof("Starting disk write permission test: %s", testFilePath)

	testDir := filepath.Dir(testFilePath)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to create test directory: %v", err)
		return fmt.Errorf("Failed to create test directory: %w", err)
	}

	if err := os.WriteFile(testFilePath, testData, 0644); err != nil {
		logger.GetLogger("dedups3").Errorf("Disk write permission test failed: %v", err)
		return fmt.Errorf("Disk write permission test failed: %w", err)
	}
	logger.GetLogger("dedups3").Infof("Disk write permission test passed")
	return nil
}

// testDiskReadPermission Test disk read permission
func testDiskReadPermission(testFilePath string, expectedData []byte) error {
	logger.GetLogger("dedups3").Infof("Starting disk read permission test: %s", testFilePath)

	data, err := os.ReadFile(testFilePath)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Disk read permission test failed: %v", err)
		return fmt.Errorf("Disk read permission test failed: %w", err)
	}

	if string(data) != string(expectedData) {
		err = errors.New("Disk data verification failed: read data does not match original data")
		logger.GetLogger("dedups3").Errorf("%v", err)
		return err
	}
	logger.GetLogger("dedups3").Infof("Disk read permission test passed")
	return nil
}

// testDiskListPermission Test disk list permission
func testDiskListPermission(testDir, testFilePath string) error {
	logger.GetLogger("dedups3").Infof("Starting disk list permission test: %s", testDir)

	entries, err := os.ReadDir(testDir)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Disk list permission test failed: %v", err)
		return fmt.Errorf("Disk list permission test failed: %w", err)
	}

	testFileName := filepath.Base(testFilePath)
	found := false
	for _, entry := range entries {
		if entry.Name() == testFileName && !entry.IsDir() {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("Disk list permission test failed: test file not found in directory list")
	}
	logger.GetLogger("dedups3").Infof("Disk list permission test passed")
	return nil
}

// testDiskDeletePermission Test disk delete permission
func testDiskDeletePermission(testFilePath string, basePath string) error {
	logger.GetLogger("dedups3").Infof("Starting disk delete permission test: %s", testFilePath)

	if err := os.Remove(testFilePath); err != nil {
		logger.GetLogger("dedups3").Errorf("Disk delete permission test failed: %v", err)
		return fmt.Errorf("Disk delete permission test failed: %w", err)
	}

	// 验证文件已被删除
	if _, err := os.Stat(testFilePath); err == nil {
		return fmt.Errorf("Disk delete permission test failed: file still exists after deletion")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("Disk delete permission verification failed: error checking file existence: %w", err)
	}
	logger.GetLogger("dedups3").Infof("Disk delete permission test passed")

	// 清理测试目录
	if err := os.RemoveAll(filepath.Join(basePath, "dedups3-test")); err != nil {
		logger.GetLogger("dedups3").Warnf("Failed to clean up test directory: %v", err)
	}
	return nil
}

// generateRandomString Generate random string
func generateRandomString(length int) string {
	data := make([]byte, length)
	_, err := rand.Read(data)
	if err != nil {
		return time.Now().Format("20060102150405")
	}
	return hex.EncodeToString(data)[:length]
}
