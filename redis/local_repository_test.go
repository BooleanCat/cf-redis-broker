package redis_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pborman/uuid"

	"github.com/pivotal-cf/cf-redis-broker/brokerconfig"
	"github.com/pivotal-cf/cf-redis-broker/redis"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Local Repository", func() {
	var (
		instanceID                string
		repo                      *redis.LocalRepository
		logger                    *lagertest.TestLogger
		tmpInstanceDataDir        = "/tmp/repotests/data"
		tmpInstanceLogDir         = "/tmp/repotests/log"
		tmpPidFileDir             = "/tmp/pidfiles"
		defaultConfigFilePath     = "/tmp/default_config_path"
		defaultConfigFileContents = []byte("daemonize yes")
	)

	BeforeEach(func() {
		instanceID = uuid.NewRandom().String()
		logger = lagertest.NewTestLogger("local-repo")

		// set up default conf file
		Expect(ioutil.WriteFile(defaultConfigFilePath, defaultConfigFileContents, os.ModePerm)).To(Succeed())

		redisConf := brokerconfig.ServiceConfiguration{
			Host:                  "127.0.0.1",
			DefaultConfigPath:     "/tmp/default_config_path",
			InstanceDataDirectory: tmpInstanceDataDir,
			PidfileDirectory:      tmpPidFileDir,
			InstanceLogDirectory:  tmpInstanceLogDir,
		}

		repo = redis.NewLocalRepository(redisConf, logger)

		Expect(os.MkdirAll(tmpInstanceDataDir, 0755)).To(Succeed())
		Expect(os.MkdirAll(tmpPidFileDir, 0755)).To(Succeed())
		Expect(os.MkdirAll(tmpInstanceLogDir, 0755)).To(Succeed())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpInstanceDataDir)).To(Succeed())
		Expect(os.RemoveAll(tmpPidFileDir)).To(Succeed())
		Expect(os.RemoveAll(tmpInstanceLogDir)).To(Succeed())
	})

	Describe("InstancePid", func() {
		Context("when a pid file exists", func() {
			instanceID := uuid.NewRandom().String()

			instance := redis.Instance{
				ID: instanceID,
			}

			BeforeEach(func() {
				pidFilePath := tmpPidFileDir + "/" + instanceID + ".pid"
				ioutil.WriteFile(pidFilePath, []byte("1234"), 0644)
			})

			It("returns its value", func() {
				pidFromFile, err := repo.InstancePid(instance.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(pidFromFile).To(Equal(1234))
			})
		})

		Context("when a pid file does not exist", func() {
			instance := redis.Instance{
				ID: uuid.NewRandom().String(),
			}

			It("returns an error", func() {
				_, err := repo.InstancePid(instance.ID)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("reading and writing instances", func() {
		Context("when the repository does not exist", func() {
			It("writes and then reads an instance", func() {
				originalInstance := newTestInstance(instanceID, repo)

				instanceFromDisk, loadInstanceErr := repo.FindByID(instanceID)
				Expect(loadInstanceErr).NotTo(HaveOccurred())

				Expect(instanceFromDisk.ID).To(Equal(originalInstance.ID))
				Expect(instanceFromDisk.Host).To(Equal(originalInstance.Host))
				Expect(instanceFromDisk.Port).To(Equal(originalInstance.Port))
				Expect(instanceFromDisk.Password).To(Equal(originalInstance.Password))
			})

			It("creates the instance data directory", func() {
				newTestInstance(instanceID, repo)

				dataDir := path.Join(tmpInstanceDataDir, instanceID, "db")
				Expect(dataDir).To(BeADirectory())
			})

			It("writes the default config file", func() {
				newTestInstance(instanceID, repo)

				configFilePath := path.Join(tmpInstanceDataDir, instanceID, "redis.conf")
				Expect(configFilePath).To(BeAnExistingFile())
			})

			It("creates the instance log directory", func() {
				newTestInstance(instanceID, repo)

				logDir := path.Join(tmpInstanceLogDir, instanceID)
				Expect(logDir).To(BeADirectory())
			})
		})

		Context("when the repository already exists", func() {
			var instance *redis.Instance

			BeforeEach(func() {
				instance = newTestInstance(instanceID, repo)
			})

			It("overwrites the config file", func() {
				originalConfigContents := []byte("my custom config")
				Expect(ioutil.WriteFile(repo.InstanceConfigPath(instance.ID), originalConfigContents, 0755)).To(Succeed())

				writeInstance(instance, repo)

				configContents, err := ioutil.ReadFile(repo.InstanceConfigPath(instance.ID))
				Expect(err).NotTo(HaveOccurred())
				Expect(configContents).NotTo(Equal(originalConfigContents))
			})

			It("does not clear the data directory", func() {
				dataFilePath := filepath.Join(repo.InstanceDataDir(instance.ID), "appendonly.aof")

				originalDataFileContents := []byte("DATA FILE")
				Expect(ioutil.WriteFile(dataFilePath, originalDataFileContents, 0755)).To(Succeed())

				writeInstance(instance, repo)

				dataFileContents, err := ioutil.ReadFile(dataFilePath)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataFileContents).To(Equal(originalDataFileContents))
			})

			It("does not clear the log directory", func() {
				logFilePath := filepath.Join(repo.InstanceLogDir(instance.ID), "redis-server.log")

				originalLogFileContents := []byte("LOG FILE")
				Expect(ioutil.WriteFile(logFilePath, originalLogFileContents, 0755)).To(Succeed())

				writeInstance(instance, repo)

				logFileContents, err := ioutil.ReadFile(logFilePath)
				Expect(err).NotTo(HaveOccurred())
				Expect(logFileContents).To(Equal(originalLogFileContents))
			})

			Context("when there is no log directory", func() {
				BeforeEach(func() {
					Expect(os.RemoveAll(repo.InstanceLogDir(instance.ID))).To(Succeed())
				})

				It("recreates the log directory", func() {
					Expect(repo.EnsureDirectoriesExist(instance)).To(Succeed())
					Expect(repo.InstanceLogDir(instance.ID)).To(BeAnExistingFile())
				})
			})
		})
	})

	Describe("FindByID", func() {
		Context("when instance does not exist", func() {
			It("returns an error", func() {
				_, err := repo.FindByID(instanceID)
				Expect(os.IsNotExist(err)).To(BeTrue())
			})
		})
	})

	Describe("InstanceExists", func() {
		Context("when instance does not exist", func() {
			It("returns false", func() {
				result, err := repo.InstanceExists(instanceID)
				Expect(result).To(BeFalse())
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when instance exists", func() {
			BeforeEach(func() {
				newTestInstance(instanceID, repo)
			})

			It("returns true", func() {
				result, err := repo.InstanceExists(instanceID)
				Expect(result).To(BeTrue())
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("Delete", func() {
		Context("When the instance exists", func() {
			BeforeEach(func() {
				newTestInstance(instanceID, repo)
			})

			It("deletes the instance data directory", func() {
				repo.Delete(instanceID)
				Expect(path.Join(tmpInstanceDataDir, instanceID)).NotTo(BeAnExistingFile())
			})

			It("deletes the instance pid file", func() {
				repo.Delete(instanceID)
				Expect(path.Join(tmpPidFileDir, instanceID+".pid")).NotTo(BeAnExistingFile())
			})

			It("deletes the instance log directory", func() {
				repo.Delete(instanceID)
				Expect(path.Join(tmpInstanceLogDir, instanceID)).NotTo(BeAnExistingFile())
			})

			It("returns no error", func() {
				Expect(repo.Delete(instanceID)).To(Succeed())
			})

			It("logs that the instance was deprovisioned", func() {
				Expect(repo.Delete(instanceID)).To(Succeed())

				Expect(logger).To(gbytes.Say("deprovision-instance"))
				expectedData := fmt.Sprintf(
					`{"instance_id":"%s","message":"Successfully deprovisioned Redis instance","plan":"shared-vm"}`, instanceID,
				)
				Expect(logger).To(gbytes.Say(expectedData))
			})
		})
	})

	Describe("InstanceCount", func() {
		Context("when there are no instances", func() {
			It("returns 0", func() {
				instanceCount, errs := repo.InstanceCount()
				Expect(errs).To(BeEmpty())
				Expect(instanceCount).To(Equal(0))
			})
		})

		Context("when there are some instances", func() {
			It("returns the correct count", func() {
				newTestInstance(instanceID, repo)

				instanceCount, errs := repo.InstanceCount()
				Expect(errs).To(BeEmpty())
				Expect(instanceCount).To(Equal(1))
			})
		})

		Context("when getting the data directories fails", func() {
			It("returns an error", func() {
				os.RemoveAll(tmpInstanceDataDir)

				_, errs := repo.InstanceCount()
				Expect(len(errs)).To(Equal(1))
				Expect(errs[0]).To(HaveOccurred())
			})
		})
	})

	Describe("AllInstancesVerbose", func() {
		It("logs that it is starting to look for shared instances, and in which directory", func() {
			repo.AllInstancesVerbose()
			Expect(logger).To(gbytes.Say(fmt.Sprintf("Starting shared instance lookup in data directory: %s", tmpInstanceDataDir)))
		})

		Context("when there are no instances", func() {
			var instances []*redis.Instance

			JustBeforeEach(func() {
				var allInstancesErrors []error
				instances, allInstancesErrors = repo.AllInstancesVerbose()
				Expect(allInstancesErrors).To(BeEmpty())
			})

			It("returns an empty instance slice", func() {
				Expect(instances).To(BeEmpty())
			})

			It("logs the instance count", func() {
				Expect(logger).To(gbytes.Say("0 shared Redis instances found"))
			})
		})

		Context("when there is one instance", func() {
			var (
				instance  *redis.Instance
				instances []*redis.Instance
				errs      []error
			)

			BeforeEach(func() {
				instance = newTestInstance(instanceID, repo)
			})

			Context("listing InstanceExists", func() {
				BeforeEach(func() {
					instances, errs = repo.AllInstancesVerbose()
				})

				It("contains created instances", func() {
					Expect(errs).To(BeEmpty())
					Expect(instances).To(ContainElement(instance))
				})

				It("logs the instance count", func() {
					Expect(logger).To(gbytes.Say("1 shared Redis instance found"))
				})

				It("logs the ID of the instance", func() {
					Expect(logger).To(gbytes.Say(fmt.Sprintf("Found shared instance: %s", instance.ID)))
				})
			})

			Context("when getting one repo ID fails", func() {
				BeforeEach(func() {
					os.Remove(repo.InstanceConfigPath(instance.ID))
					_, errs = repo.AllInstances()
				})

				It("returns one error", func() {
					Expect(len(errs)).To(Equal(1))
					Expect(errs[0]).To(HaveOccurred())
				})

				It("logs the error", func() {
					Expect(logger).To(gbytes.Say(errs[0].Error()))
					Expect(logger).To(gbytes.Say(fmt.Sprintf("Error getting instance details for instance ID: %s", instanceID)))
				})
			})
		})

		Context("when there are several instances", func() {
			var (
				instanceIDs []string
				instances   []*redis.Instance
			)

			BeforeEach(func() {
				for i := 0; i < 3; i++ {
					instanceIDs = append(instanceIDs, uuid.NewRandom().String())
				}
				sort.Strings(instanceIDs)
				for _, instanceID := range instanceIDs {
					instances = append(instances, newTestInstance(instanceID, repo))
				}
			})

			AfterEach(func() {
				instanceIDs = []string{}
				instances = []*redis.Instance{}
			})

			It("logs the instance count", func() {
				_, errs := repo.AllInstancesVerbose()
				Expect(errs).To(BeEmpty())
				Expect(logger).To(gbytes.Say("3 shared Redis instances found"))
			})

			Context("when getting one repo ID fails", func() {
				var errs []error

				BeforeEach(func() {
					os.Remove(repo.InstanceConfigPath(instanceIDs[0]))
					instances, errs = repo.AllInstances()
				})

				It("returns one error", func() {
					Expect(len(errs)).To(Equal(1))
					Expect(errs[0]).To(HaveOccurred())
				})

				It("returns the other two instances", func() {
					Expect(len(instances)).To(Equal(2))
				})
			})
		})

		It("does not contain deleted instances", func() {
			instance := newTestInstance(instanceID, repo)
			repo.Delete(instanceID)

			instances, errs := repo.AllInstances()
			Expect(errs).To(BeEmpty())
			Expect(instances).NotTo(ContainElement(instance))
		})

		Context("when getting the data directories fails", func() {
			It("returns an error", func() {
				os.RemoveAll(tmpInstanceDataDir)

				_, errs := repo.AllInstances()
				Expect(len(errs)).To(Equal(1))
				Expect(errs[0]).To(HaveOccurred())
			})

			It("logs the error", func() {
				os.RemoveAll(tmpInstanceDataDir)

				_, errs := repo.AllInstances()

				Expect(logger).To(gbytes.Say(errs[0].Error()))
				Expect(logger).To(gbytes.Say("Error finding shared instances"))
			})
		})
	})

	Describe("AllInstances", func() {
		It("doesn't log that it is starting to look for shared instances, and in which directory", func() {
			repo.AllInstances()
			Expect(logger).NotTo(gbytes.Say(fmt.Sprintf("Starting shared instance lookup in data directory: %s", tmpInstanceDataDir)))
		})

		Context("when there are no instances", func() {
			It("returns an empty instance slice", func() {
				instances, errs := repo.AllInstances()
				Expect(errs).To(BeEmpty())
				Expect(instances).To(BeEmpty())
			})

			It("doesn't log the instance count", func() {
				Expect(logger).NotTo(gbytes.Say("0 shared Redis instances found"))
			})
		})

		Context("when there is one instance", func() {
			var (
				instance  *redis.Instance
				instances []*redis.Instance
				errs      []error
			)

			BeforeEach(func() {
				instance = newTestInstance(instanceID, repo)
			})

			Context("listing InstanceExists", func() {
				BeforeEach(func() {
					instances, errs = repo.AllInstances()
				})

				It("contains created instances", func() {
					Expect(errs).To(BeEmpty())
					Expect(instances).To(ContainElement(instance))
				})

				It("doesn't log the ID of the instance", func() {
					Expect(logger).NotTo(gbytes.Say(fmt.Sprintf("Found shared instance: %s", instance.ID)))
				})
			})

			Context("when getting one repo ID fails", func() {
				BeforeEach(func() {
					os.Remove(repo.InstanceConfigPath(instance.ID))
					_, errs = repo.AllInstances()
				})

				It("returns one error", func() {
					Expect(len(errs)).To(Equal(1))
					Expect(errs[0]).To(HaveOccurred())
				})

				It("logs the error", func() {
					Expect(logger).To(gbytes.Say(errs[0].Error()))
					Expect(logger).To(gbytes.Say(fmt.Sprintf("Error getting instance details for instance ID: %s", instanceID)))
				})
			})
		})

		Context("when there are several instances", func() {
			var (
				instanceIDs []string
				instances   []*redis.Instance
			)

			BeforeEach(func() {
				for i := 0; i < 3; i++ {
					instanceIDs = append(instanceIDs, uuid.NewRandom().String())
				}
				sort.Strings(instanceIDs)
				for _, instanceID := range instanceIDs {
					instances = append(instances, newTestInstance(instanceID, repo))
				}
			})

			AfterEach(func() {
				instanceIDs = []string{}
				instances = []*redis.Instance{}
			})

			It("doesn't log the instance count", func() {
				_, errs := repo.AllInstances()
				Expect(errs).To(BeEmpty())
				Expect(logger).NotTo(gbytes.Say("3 shared Redis instances found"))
			})

			Context("when getting one repo ID fails", func() {
				var errs []error

				BeforeEach(func() {
					os.Remove(repo.InstanceConfigPath(instanceIDs[0]))
					instances, errs = repo.AllInstances()
				})

				It("returns one error", func() {
					Expect(len(errs)).To(Equal(1))
					Expect(errs[0]).To(HaveOccurred())
				})

				It("returns the other two instances", func() {
					Expect(len(instances)).To(Equal(2))
				})
			})
		})

		It("does not contain deleted instances", func() {
			instance := newTestInstance(instanceID, repo)
			repo.Delete(instanceID)

			instances, errs := repo.AllInstances()
			Expect(errs).To(BeEmpty())
			Expect(instances).NotTo(ContainElement(instance))
		})

		Context("when getting the data directories fails", func() {
			It("returns an error", func() {
				os.RemoveAll(tmpInstanceDataDir)

				_, errs := repo.AllInstances()
				Expect(len(errs)).To(Equal(1))
				Expect(errs[0]).To(HaveOccurred())
			})

			It("logs the error", func() {
				os.RemoveAll(tmpInstanceDataDir)

				_, errs := repo.AllInstances()

				Expect(logger).To(gbytes.Say(errs[0].Error()))
				Expect(logger).To(gbytes.Say("Error finding shared instances"))
			})
		})
	})
})

var _ = Describe("Setup", func() {
	var (
		repo              *redis.LocalRepository
		instanceID        string
		logger            *lagertest.TestLogger
		tmpConfigFilePath = "/tmp/default_config_path"
		tmpDataDir        = "/tmp/repotests/data"
		tmpPidfileDir     = "/tmp/repotests/pids"
		tmpLogDir         = "/tmp/repotests/log"
		instance          redis.Instance
	)

	BeforeEach(func() {
		Expect(os.MkdirAll(tmpDataDir, 0755)).To(Succeed())
		Expect(os.MkdirAll(tmpPidfileDir, 0755)).To(Succeed())
		Expect(os.MkdirAll(tmpLogDir, 0755)).To(Succeed())
		_, createFileErr := os.Create(tmpConfigFilePath)
		Expect(createFileErr).NotTo(HaveOccurred())

		instanceID = uuid.NewRandom().String()
		logger = lagertest.NewTestLogger("local-repo-setup")

		instance = redis.Instance{
			ID: instanceID,
		}

		redisConf := brokerconfig.ServiceConfiguration{
			Host:                  "127.0.0.1",
			DefaultConfigPath:     tmpConfigFilePath,
			InstanceDataDirectory: tmpDataDir,
			PidfileDirectory:      tmpPidfileDir,
			InstanceLogDirectory:  tmpLogDir,
		}

		repo = redis.NewLocalRepository(redisConf, logger)
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDataDir)).To(Succeed())
		Expect(os.RemoveAll(tmpPidfileDir)).To(Succeed())
		Expect(os.RemoveAll(tmpLogDir)).To(Succeed())
	})

	Context("When setup is successful", func() {
		It("creates the appropriate directories", func() {
			tmpInstanceDataDir := path.Join(tmpDataDir, instanceID)
			tmpInstanceLogDir := path.Join(tmpLogDir, instanceID)
			Expect(tmpInstanceDataDir).NotTo(BeADirectory())
			Expect(tmpInstanceLogDir).NotTo(BeADirectory())

			Expect(repo.Setup(&instance)).To(Succeed())
			Expect(tmpInstanceDataDir).To(BeADirectory())
			Expect(tmpInstanceLogDir).To(BeADirectory())
		})

		It("creates a lock file", func() {
			Expect(repo.Setup(&instance)).To(Succeed())

			lockFilePath := path.Join(tmpDataDir, instanceID, "lock")
			Expect(lockFilePath).To(BeAnExistingFile())
		})

		It("writes the config file", func() {
			Expect(repo.Setup(&instance)).To(Succeed())

			configFilePath := path.Join(tmpDataDir, instanceID, "redis.conf")

			configFileContent, err := ioutil.ReadFile(configFilePath)
			Expect(err).NotTo(HaveOccurred())

			redisServerName := "redis-server-" + instanceID
			Expect(configFileContent).To(ContainSubstring(redisServerName))
		})

		It("logs that the instance was provisioned", func() {
			Expect(repo.Setup(&instance)).To(Succeed())

			Expect(logger).To(gbytes.Say("provision-instance"))
			expectedData := fmt.Sprintf(
				`{"instance_id":"%s","message":"Successfully provisioned Redis instance","plan":"shared-vm"}`, instanceID,
			)
			Expect(logger).To(gbytes.Say(expectedData))
		})
	})

	Context("When setup is not successful", func() {
		Context("the instance dir does not have write permissions", func() {
			BeforeEach(func() {
				Expect(os.Chmod(tmpDataDir, 0400)).To(Succeed())
				Expect(os.Chmod(tmpLogDir, 0400)).To(Succeed())
			})

			It("returns an error", func() {
				Expect(repo.Setup(&instance)).NotTo(Succeed())
			})

			It("logs the error", func() {
				_ = repo.Setup(&instance)

				Expect(logger).To(gbytes.Say("local-repo-setup.ensure-dirs-exist"))
				Expect(logger).To(gbytes.Say("permission denied"))
			})

			AfterEach(func() {
				Expect(os.Chmod(tmpDataDir, 0755)).To(Succeed())
				Expect(os.Chmod(tmpLogDir, 0755)).To(Succeed())
			})
		})

		Context("the config file being written is invalid", func() {
			BeforeEach(func() {
				invalidConfigFilePath := "/tmp/invalid_config_path"
				invalidConfigFileContents := []byte("notavalidconfig")

				Expect(ioutil.WriteFile(invalidConfigFilePath, invalidConfigFileContents, os.ModePerm)).To(Succeed())

				repo.RedisConf.DefaultConfigPath = invalidConfigFilePath
			})

			It("returns an error", func() {
				Expect(repo.Setup(&instance)).NotTo(Succeed())
			})
		})
	})
})

func newTestInstance(instanceID string, repo *redis.LocalRepository) *redis.Instance {
	instance := &redis.Instance{
		ID:   instanceID,
		Host: "127.0.0.1",
		Port: 8080,
	}
	writeInstance(instance, repo)
	return instance
}

func writeInstance(instance *redis.Instance, repo *redis.LocalRepository) {
	Expect(repo.EnsureDirectoriesExist(instance)).To(Succeed())
	Expect(repo.WriteConfigFile(instance)).To(Succeed())
	pid := []byte("1234")
	Expect(ioutil.WriteFile(repo.InstancePidFilePath(instance.ID), pid, 0644)).To(Succeed())
}
