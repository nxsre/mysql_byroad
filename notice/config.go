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

type Config struct {
	User      string
	Password  string
	SmsAddr   string
	EmailAddr string
}

func NewSmsConfig() *SmsConfig {
	return &SmsConfig{}
}

func NewEmailConfig() *EmailConfig {
	return &EmailConfig{}
}

var smsNotice *SmsNoticer
var emailNotice *EmailNoticer

func Init(config *Config) {
	smsConfig := SmsConfig{
		User:     config.User,
		Password: config.Password,
		Addr:     config.SmsAddr,
	}
	smsNotice = NewSmsNoticer(&smsConfig)
	emaiConfig := EmailConfig{
		User:     config.User,
		Password: config.Password,
		Addr:     config.EmailAddr,
	}
	emailNotice = NewEmailNoticer(&emaiConfig)
}

func SendSms(number, content string) (string, error) {
	return smsNotice.SendSms(number, content)
}

func SendEmail(recipient, subject, content string) (string, error) {
	return emailNotice.SendEmail(recipient, subject, content)
}
