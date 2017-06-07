package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/newhook/workers/httpworkers/data"
)

var (
	url    = "http://localhost:8080"
	client = &http.Client{}
)

func Fetch(queue string) (data.Job, bool, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/fetch/%s", url, queue), nil)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return data.Job{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return data.Job{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return data.Job{}, false, errors.New(fmt.Sprintf("error: %d", resp.StatusCode))
	}

	body, _ := ioutil.ReadAll(resp.Body)
	var job data.Job
	if err := json.Unmarshal(body, &job); err != nil {
		return data.Job{}, false, err
	}
	fmt.Println("returning job", job)
	return job, true, nil
}

func Ack(j data.Job) (bool, error) {
	request := data.Request{
		ID:    j.ID,
		Token: j.Token,
		Env:   j.Env,
		Queue: j.Queue,
	}
	b, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/ack", url), bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(fmt.Sprintf("error: %d", resp.StatusCode))
	}
	return true, nil
}

func Ping(j data.Job) (bool, error) {
	request := data.Request{
		ID:    j.ID,
		Token: j.Token,
		Env:   j.Env,
		Queue: j.Queue,
	}
	b, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/ping", url), bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(fmt.Sprintf("error: %d", resp.StatusCode))
	}
	return true, nil
}
