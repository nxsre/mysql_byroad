package notice

type SmsConfig struct {
	User     string
	Password string
	Addr     string
}

type EmailConfig struct {
	User     string
	Password string
	Addr     string
}

func NewSmsConfig() *SmsConfig {
	return &SmsConfig{
		User:     "int_notice",
		Password: "notice_rt902pnkl10udnq",
		Addr:     "http://sms.int.jumei.com/index.php",
	}
}

func NewEmailConfig() *EmailConfig {
	return &EmailConfig{
		User:     "int_notice",
		Password: "notice_rt902pnkl10udnq",
		Addr:     "http://email.int.jumei.com/send.php",
	}
}
