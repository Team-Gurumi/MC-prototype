package p2p

// libp2p protocol ID (versioned for future compatibility)
const ProtoGet = "/mc-get/1.0.0"

// Request carries only the root CID. (range, piece, merkle path, etc. planned for later)
type GetRequest struct {
	RootCID string `json:"root_cid"`
}

// Response header (optional): total length, mime, etc. (currently body is streamed as-is)
type GetResponse struct {
	Size int64  `json:"size,omitempty"`
	Err  string `json:"err,omitempty"`
}
