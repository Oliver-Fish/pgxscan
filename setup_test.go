package pgxscan

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
)

var db *pgxpool.Pool

func TestMain(m *testing.M) {
	conURL, err := getConnectionURL(
		"localhost",
		"5430",
		"postgres",
		"postgres",
		"password1",
		"disable",
	)
	if err != nil {
		panic(err)
	}

	config, err := pgxpool.ParseConfig(conURL)
	if err != nil {
		panic(err)
	}

	db, err = pgxpool.ConnectConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}

	//Launch tests as normal
	code := m.Run()
	os.Exit(code)
}

func getConnectionURL(hostname, port, database, username, password, sslmode string) (string, error) {
	switch {
	case hostname == "":
		return "", errors.New("missing hostname")
	case port == "":
		return "", errors.New("missing port")
	case database == "":
		return "", errors.New("missing database")
	case username == "":
		return "", errors.New("missing username")
	case password == "":
		return "", errors.New("missing password")
	case sslmode == "":
		return "", errors.New("missing sslmode")
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", username, password, hostname, port, database, sslmode), nil
}
