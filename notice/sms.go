package notice

import (
	"io/ioutil"
	"net/http"
	"net/url"
)

type SmsNoticer struct {
	config *SmsConfig
}

func (this *SmsNoticer) SendSms(num string, content string) (string, error) {
	payload := this.newPayload(num, content)
	resp, err := http.PostForm(this.config.Addr, payload)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (this *SmsNoticer) newPayload(num string, content string) url.Values {
	v := url.Values{}
	v.Add("task", this.config.User)
	v.Add("key", this.config.Password)
	v.Add("num", num)
	v.Add("content", content)
	return v
}

func NewSmsNoticer(config *SmsConfig) *SmsNoticer {
	sn := SmsNoticer{}
	if config == nil {
		sn.config = NewSmsConfig()
	} else {
		sn.config = config
	}
	return &sn
}
