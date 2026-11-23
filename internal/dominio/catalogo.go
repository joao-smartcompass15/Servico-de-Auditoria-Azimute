package dominio

// RegraAcao diz como tratar uma ação. Vem do arquivo de catálogo (JSON).
type RegraAcao struct {
	Distribuir        bool `json:"distribuir"`         // além de auditar, avisa os usuários?
	MotivoObrigatorio bool `json:"motivo_obrigatorio"` // o "por quê" tem de vir preenchido?
}

// Catalogo mapeia o nome da ação para a sua regra.
// map[chave]valor é o dicionário do Go.
type Catalogo map[string]RegraAcao

// Regra devolve a regra da ação e se ela existe no catálogo.
// Ação fora do catálogo devolve a regra "zero": não distribui, motivo opcional.
func (c Catalogo) Regra(acao string) (RegraAcao, bool) {
	regra, existe := c[acao] // "comma ok": existe diz se a chave está no map
	return regra, existe
}
