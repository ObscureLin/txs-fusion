package bcfl

import "encoding/hex"

const (
	SetUpdates = "19e13d20"
	// Download=""
	// Aggregate=""
	ParentLinkTag = 1024
)

func HexToByteArray(hexStr string) ([]byte, error) {
	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr
	}
	buf := make([]byte, hex.DecodedLen(len(hexStr)))
	n, err := hex.Decode(buf, []byte(hexStr))
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
