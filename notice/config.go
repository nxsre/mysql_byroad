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
	return &SmsConfig{}
}

func NewEmailConfig() *EmailConfig {
	return &EmailConfig{}
}
