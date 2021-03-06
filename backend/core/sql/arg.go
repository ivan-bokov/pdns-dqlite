package sql

import (
	"github.com/pkg/errors"
)

func ArgToMap(args ...interface{}) (map[string]interface{}, error) {
	if len(args)%2 != 0 {
		return nil, errors.New("Количество аргументов не четное") //TODO Перевести на английский
	}
	result := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		switch v := args[i].(type) {
		case string:
			result[v] = args[i+1]
		}
	}
	return result, nil
}
