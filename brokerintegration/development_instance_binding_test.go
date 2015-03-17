package brokerintegration_test

import (
	"encoding/json"

	"code.google.com/p/go-uuid/uuid"
	redigo "github.com/garyburd/redigo/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Shared instance binding", func() {

	var instanceID string
	var bindingID string
	var httpInputs HTTPExampleInputs

	BeforeEach(func() {
		instanceID = uuid.NewRandom().String()
		bindingID = uuid.NewRandom().String()
		httpInputs = HTTPExampleInputs{
			Method: "PUT",
			URI:    bindingURI(instanceID, bindingID),
		}
	})

	Context("when the instance already exists", func() {
		BeforeEach(func() {
			code, _ := provisionInstance(instanceID, "shared")
			Ω(code).To(Equal(201))
		})

		AfterEach(func() {
			deprovisionInstance(instanceID)
		})

		HTTPResponseShouldContainExpectedHTTPStatusCode(&httpInputs, 201)

		Describe("the redis instance", func() {
			var client redigo.Conn

			BeforeEach(func() {
				_, body := bindInstance(instanceID, bindingID)

				var parsedJSON map[string]interface{}
				json.Unmarshal(body, &parsedJSON)

				credentials := parsedJSON["credentials"].(map[string]interface{})
				password := credentials["password"].(string)
				Ω(password).ToNot(BeEquivalentTo(""))

				port := uint(credentials["port"].(float64))
				host := credentials["host"].(string)

				client = BuildRedisClient(port, host, password)
			})

			AfterEach(func() {
				client.Close()
			})

			It("is connectable", func() {
				ret, err := redigo.String(client.Do("PING"))
				Ω(err).ShouldNot(HaveOccurred())
				Ω(ret).Should(Equal("PONG"))
			})

			It("has the correct configuration", func() {
				var configResponse struct {
					MaxMemory string `redis:"maxmemory"`
				}

				ret, err := redigo.Values(client.Do("abc123", "GET", "maxmemory"))
				Ω(err).NotTo(HaveOccurred())
				err = redigo.ScanStruct(ret, &configResponse)
				Ω(err).NotTo(HaveOccurred())
				Ω(configResponse.MaxMemory).To(Equal("52428800"))
			})
		})
	})

	Context("when the instance does not already exist", func() {
		HTTPResponseShouldContainExpectedHTTPStatusCode(&httpInputs, 404)

		HTTPResponseShouldContainBrokerErrorMessage(&httpInputs, "instance does not exist")
	})
})
