package logutil

import "go.uber.org/zap"

var logger *zap.Logger

func init() {
	logger = zap.NewExample()
}

func Debug(msg string, field ...zap.Field) {
	logger.Debug(msg, field...)
}

func Info(msg string, field ...zap.Field) {
	logger.Info(msg, field...)
}

func Warn(msg string, field ...zap.Field) {
	logger.Warn(msg, field...)
}

func Error(msg string, field ...zap.Field) {
	logger.Error(msg, field...)
}

func Fatal(msg string, field ...zap.Field) {
	logger.Fatal(msg, field...)
}

func Panic(msg string, field ...zap.Field) {
	logger.Panic(msg, field...)
}
