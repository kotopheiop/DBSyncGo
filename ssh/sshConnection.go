package sshConnection

import (
	"DBSyncGo/config"
	"fmt"
	"github.com/jfcote87/sshdb"
	"golang.org/x/crypto/ssh"
	"os"
)

func createSSHClientConfig(cfg config.Config) (*ssh.ClientConfig, error) {
	key, err := os.ReadFile(cfg.SSHKeyPath)
	if err != nil {
		return nil, fmt.Errorf("⛔ Не удалось прочитать приватный ключ: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	sshClientConfig := &ssh.ClientConfig{
		User:            cfg.SSHUser,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	return sshClientConfig, nil
}

func CreateSSHClient(cfg config.Config) (*ssh.Client, error) {
	sshClientConfig, err := createSSHClientConfig(cfg)
	if err != nil {
		return nil, err
	}

	client, err := ssh.Dial("tcp", cfg.RemoteServer, sshClientConfig)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func CreateSSHTunnel(cfg config.Config) (*sshdb.Tunnel, error) {
	sshClientConfig, err := createSSHClientConfig(cfg)
	if err != nil {
		return nil, err
	}

	tunnel, err := sshdb.New(sshClientConfig, cfg.RemoteServer)
	if err != nil {
		return nil, fmt.Errorf("⛔ Не удалось создать новый туннель: %v", err)
	}

	return tunnel, nil
}
