package db

type TableManager struct {
	Id  map[string]map[uint16]uint64
	Limit uint16
	TurnIdx map[string]uint16
}

var TabManager TableManager

func TablePrimaryIdForRegion(ridx uint16,tableName string)(turnIdx,limit uint16,id uint64){
	id = TabManager.Id[tableName][ridx]
	limit = TabManager.Limit
	turnIdx = TabManager.TurnIdx[tableName]
	TabManager.Id[tableName][ridx] = id + limit
	TabManager.TurnIdx[tableName] = turnIdx + 1
	return
}