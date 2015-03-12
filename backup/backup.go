package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/pivotal-cf/cf-redis-broker/backup/s3bucket"
	"github.com/pivotal-cf/cf-redis-broker/brokerconfig"
	"github.com/pivotal-cf/cf-redis-broker/redis/client"
	"github.com/pivotal-cf/cf-redis-broker/redisconf"
	"github.com/pivotal-golang/lager"
)

type Backup struct {
	Config *brokerconfig.Config
	Logger lager.Logger
}

func (backup Backup) Create(instancePath, instanceID string) error {
	bucket, err := backup.getOrCreateBucket()
	if err != nil {
		return err
	}

	if err = backup.createSnapshot(instancePath); err != nil {
		return err
	}

	pathToRdbFile := path.Join(instancePath, "db", "dump.rdb")

	if !fileExists(pathToRdbFile) {
		backup.Logger.Info("dump.rdb not found, skipping instance backup", lager.Data{
			"Local file": pathToRdbFile,
		})
		return nil
	}

	return backup.uploadToS3(instanceID, pathToRdbFile, bucket)
}

func (backup Backup) getOrCreateBucket() (s3bucket.Bucket, error) {
	s3Client := s3bucket.NewClient(
		backup.Config.RedisConfiguration.BackupConfiguration.EndpointUrl,
		backup.Config.RedisConfiguration.BackupConfiguration.S3Region,
		backup.Config.RedisConfiguration.BackupConfiguration.AccessKeyId,
		backup.Config.RedisConfiguration.BackupConfiguration.SecretAccessKey,
	)

	return s3Client.GetOrCreate(backup.Config.RedisConfiguration.BackupConfiguration.BucketName)
}

func (backup Backup) createSnapshot(instancePath string) error {
	client, err := backup.buildRedisClient(instancePath)
	if err != nil {
		return err
	}

	return client.CreateSnapshot(backup.Config.RedisConfiguration.BackupConfiguration.BGSaveTimeoutSeconds)
}

func (backup Backup) buildRedisClient(instancePath string) (*client.Client, error) {
	instanceConfPath := path.Join(instancePath, "redis.conf")
	instanceConf, err := redisconf.Load(instanceConfPath)
	if err != nil {
		return nil, err
	}

	port, err := ioutil.ReadFile(path.Join(instancePath, "redis-server.port"))
	if err != nil {
		return nil, err
	}
	instanceConf.Set("port", string(port))

	password, err := ioutil.ReadFile(path.Join(instancePath, "redis-server.password"))
	if err != nil {
		return nil, err
	}
	instanceConf.Set("requirepass", string(password))

	return client.Connect(backup.Config.RedisConfiguration.Host, instanceConf)
}

func (backup Backup) uploadToS3(instanceID, pathToRdbFile string, bucket s3bucket.Bucket) error {
	rdbBytes, err := ioutil.ReadFile(pathToRdbFile)
	if err != nil {
		return err
	}

	remotePath := fmt.Sprintf("%s/%s", backup.Config.RedisConfiguration.BackupConfiguration.Path, instanceID)

	backup.Logger.Info("Backing up instance", lager.Data{
		"Local file":  pathToRdbFile,
		"Remote file": remotePath,
	})

	return bucket.Upload(rdbBytes, remotePath)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}