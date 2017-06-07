package data

type Request struct {
	ID    int    `json:"id"`
	Token string `json:"token"`
	Env   int    `json:"environment_id"`
	Queue string `json:"queue"`
}
type Job struct {
	ID    int    `json:"id"`
	Token string `json:"token"`
	Env   int    `json:"environment_id"`
	Queue string `json:"queue"`
	Data  []byte `json:"data"`
}
