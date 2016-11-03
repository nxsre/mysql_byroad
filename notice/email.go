package notice

import (
	"io/ioutil"
	"net/http"
	"net/url"
)

type EmailNoticer struct {
	config *EmailConfig
}

func (this *EmailNoticer) SendEmail(recipient, subject, content string) (string, error) {
	payload := this.newPayload(recipient, subject, content)
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

func (this *EmailNoticer) newPayload(recipient, subject, content string) url.Values {
	v := url.Values{}
	v.Add("task", this.config.User)
	v.Add("key", this.config.Password)
	v.Add("email_destinations", recipient)
	v.Add("email_subject", subject)
	v.Add("email_content", content)
	return v
}

func NewEmailNoticer(config *EmailConfig) *EmailNoticer {
	en := EmailNoticer{}
	if config == nil {
		en.config = NewEmailConfig()
	} else {
		en.config = config
	}
	return &en
}
