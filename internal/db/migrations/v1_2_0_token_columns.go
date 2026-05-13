package db

import (
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type requestLogTokenMigration struct{}

func (requestLogTokenMigration) TableName() string {
	return "request_logs"
}

func V1_2_0_AddTokenAndResponseColumns(db *gorm.DB) error {
	migrator := db.Migrator()
	tbl := &requestLogTokenMigration{}

	if !migrator.HasColumn(tbl, "prompt_tokens") {
		if err := db.Exec("ALTER TABLE request_logs ADD COLUMN prompt_tokens INTEGER NOT NULL DEFAULT 0").Error; err != nil {
			logrus.Debugf("v1.2.0: prompt_tokens may already exist: %v", err)
		}
	}

	if !migrator.HasColumn(tbl, "completion_tokens") {
		if err := db.Exec("ALTER TABLE request_logs ADD COLUMN completion_tokens INTEGER NOT NULL DEFAULT 0").Error; err != nil {
			logrus.Debugf("v1.2.0: completion_tokens may already exist: %v", err)
		}
	}

	if !migrator.HasColumn(tbl, "response_body") {
		if err := db.Exec("ALTER TABLE request_logs ADD COLUMN response_body TEXT").Error; err != nil {
			logrus.Debugf("v1.2.0: response_body may already exist: %v", err)
		}
	}

	logrus.Info("Migration v1.2.0: Token and response columns ready.")
	return nil
}
