package devicetypes

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

func GetDeviceType(id string, url string) (DeviceType, error) {
	url += "/device-types/" + id
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return DeviceType{}, err
	}
	req.Header.Set("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwi"+
		"bmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c") // John Doe
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return DeviceType{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return DeviceType{}, errors.New("unexpected status code while getting device type: " + strconv.Itoa(resp.StatusCode) + ", URL was " + url)
	}
	var dt DeviceType
	err = json.NewDecoder(resp.Body).Decode(&dt)
	if err != nil {
		return DeviceType{}, err
	}
	return dt, nil
}
