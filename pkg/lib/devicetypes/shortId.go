package devicetypes

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
)

func ShortenId(longId string) (string, error) {
	parts := strings.Split(longId, ":")
	noPrefix := parts[len(parts)-1]
	noPrefix = strings.ReplaceAll(noPrefix, "-", "")
	bytes, err := hex.DecodeString(noPrefix)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}
