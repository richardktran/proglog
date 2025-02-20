package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
)

func configFile(fileName string) string {
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, fileName)
	}

	homeDir, err := os.UserHomeDir()

	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, ".proglog", fileName)
}
