package attestation

type Version struct {
	CID       string    `json:"cid"`
	Height    uint      `json:"height"`
	Signature Signature `json:"signature"`
}
