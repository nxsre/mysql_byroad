package main

import (
	"mysql_byroad/notice"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func InitAlert(config AlertConfig) {
	c := notice.Config{
		User:      config.User,
		Password:  config.Password,
		SmsAddr:   config.SmsAddr,
		EmailAddr: config.EmailAddr,
	}
	notice.Init(&c)
}

func SendAlert(content string) {
	var phoneNumbers, emails []string
	if Conf.AlertConfig.PhoneNumbers != "" {
		phoneNumbers = strings.Split(Conf.AlertConfig.PhoneNumbers, ";")
	}
	if Conf.AlertConfig.Emails != "" {
		emails = strings.Split(Conf.AlertConfig.Emails, ";")
	}
	for _, number := range phoneNumbers {
		number = strings.TrimSpace(number)
		ret, err := notice.SendSms(number, content)
		log.Infof("send sms %s: %s,\nret: %s error: %s", number, content, ret, err)
	}
	for _, email := range emails {
		email = strings.TrimSpace(email)
		ret, err := notice.SendEmail(email, "旁路系统", content)
		log.Infof("send email %s: %s,\nret: %s error: %s", email, content, ret, err)
	}
}
