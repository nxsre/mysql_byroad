package notice

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type EmailNoticer struct {
	config *EmailConfig
}

func (this *EmailNoticer) SendEmail(recipient, subject, content string) (string, error) {
	payload := this.newPayload(recipient, subject, content)
	reader := strings.NewReader(payload)
	resp, err := http.Post(this.config.Addr, "text/html", reader)
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

func (this *EmailNoticer) newPayload(recipient, subject, content string) string {
	v := url.Values{}
	v.Add("task", this.config.User)
	v.Add("key", this.config.Password)
	v.Add("email_destinations", recipient)
	v.Add("email_subject", subject)
	v.Add("email_content", content)
	return v.Encode()
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
