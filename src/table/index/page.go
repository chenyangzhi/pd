package index

import (
	"common"
	"encoding/binary"
	"log"
	"container/list"
	"github.com/edsrzf/mmap-go"
	"os"
	"path"
)

const (
	BTNPGHEADERLENGHT = common.INT16_LEN + common.INT64_LEN +
		common.INT64_LEN + common.INT32_LEN
	PAGESIZE = 4096
	MMAPSIZE = 1000
	METAPAGEMAXLENGTH = 16*4096
	DEGREE = 110
)

var EmptyPage = list.New()
var MMapmap= make(map[int64]mmap.MMap)
var MtPage = NewMetaPage(0,0)
var DirtyPage = make(common.Set)
var TablePath map[string]TablePath
var TableFileIo map[string]os.File
type MetaPage struct {
	RootId int64
	EmptyPageCount int64
	EmptyPage []byte
	BTreePageCount int64
}
func NewMetaPage(rootId int32,emptyCount int32)*MetaPage{
	return &MetaPage{
		RootId:rootId,
		EmptyPageCount:emptyCount,
		EmptyPage:make([]int32,0,emptyCount),
	}
}

type BtreeNodePageHeaderData struct {
	PageSize    int32
	FreePointer int64
	PageVersion int16
	BtreeNodeId int64
}
type BtreeNodeItem struct {
	Key     string
	IdxId   int64
	KeyType byte
}
func NewBtreeNodeItem(key string,idxId int64, keyType byte){
	
}
type BtreeNodePage struct {
	PageHeader  *BtreeNodePageHeaderData
	Items       *[]BtreeNodeItem
	Children    []int64
	ItemsLength int32
}

func (item BtreeNodeItem) Size() int32 {
	return int32(len(item.Key) + common.BYTE_LEN +
		common.INT16_LEN + common.INT64_LEN +
		common.INT64_LEN)
}
func (pgHeader BtreeNodePageHeaderData) Size() int32 {
	return BTNPGHEADERLENGHT
}

func (item BtreeNodePageHeaderData) ToBytes(bytes *[]byte) {
	iStart, iEnd := 0, 0
	bs := make([]byte, 8)
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs, uint32(item.PageSize))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.FreePointer))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, uint16(item.PageVersion))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.BtreeNodeId))
	copy((*bytes)[iStart:iEnd], bs)
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, crc)
	copy((*bytes)[iStart:iEnd], bs)
}

func (item BtreeNodeItem) ToBytes(bytes *[]byte) {
	length := item.Size()
	bs := make([]byte, 8)
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint32(bs, uint32(length))
	copy((*bytes)[iStart:iEnd], bs)
	keyLen := len(item.Key)
	iStart = iEnd
	iEnd = iStart + keyLen
	copy((*bytes)[iStart:iEnd], []byte(item.Key))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.IdxId))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.BYTE_LEN
	(*bytes)[iStart] = item.KeyType
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, crc)
	copy((*bytes)[iStart:iEnd], bs)
}

func BytesToBtreeNodeItems(barr *[]byte, count int16) []BtreeNodeItem {
	items := make([]BtreeNodeItem, count, count)
	iStart, iEnd := int32(0), int32(0)
	sentiel := 0
	for i := int16(0); i < count; i++ {
		iEnd = iStart + common.INT16_LEN
		length := int32(binary.LittleEndian.Uint32((*barr)[iStart:iEnd]))
		iStart = iEnd
		iEnd = iStart + length
		items[i].Key = string((*barr)[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT64_LEN
		items[i].IdxId = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
		iStart = iEnd
		iEnd = iStart + common.BYTE_LEN
		items[i].KeyType = (*barr)[iStart]
		crc_0 := common.Crc16((*barr)[sentiel:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
		if crc_0 != crc_1 {
			log.Fatalf("the crc is failed")
		}
		sentiel = int(iEnd)
	}
	return items
}

func BytesToBtreeNodePageHeader(barr *[]byte) *BtreeNodePageHeaderData {
	iStart, iEnd := 0, 0
	item := new(BtreeNodePageHeaderData)
	iEnd = iStart + common.INT32_LEN
	item.PageSize = int32(binary.LittleEndian.Uint32((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.FreePointer = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = int16(binary.LittleEndian.Uint16((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.BtreeNodeId = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	crc_0 := common.Crc16((*barr)[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item
}
func BatchBtreeNodeItemToBytes(items *[]BtreeNodeItem, size int32) []byte {
	bytes := make([]byte, size, size)
	iStart, iEnd := int32(0), int32(0)
	for _, item := range *items {
		length := item.Size()
		iStart = iEnd
		iEnd = iStart + length
		arr := bytes[iStart:iEnd]
		item.ToBytes(&arr)
		copy(bytes[iStart:iEnd], arr)
	}
	return bytes
}

//func BtreeNodePageToBytes(btreeNodePage BtreeNodePage)[]byte{
//	PageHeader  *BtreeNodePageHeaderData
//	Items       *[]BtreeNodeItem
//	Children    []int64
//	ItemsLength int32
//}
type TablePath struct {
	basePath string
	table string
	dataBase string
	columnName string
}
func CreateFileDs(basePath, table, database, columnName string){
	for key,value := range TablePath{
		filePath := path.Join(value.basePath, value.dataBase, value.table, value.columnName)
		f, err := os.OpenFile(filePath, os.O_RDWR, 0666)
		common.Check(err)
		key = key + "." + columnName
		TableFileIo[key] = f
	}
}
func MMapPage(columnName string)*mmap.MMap{   // columnName join database, table and columnName with .
        f := TableFileIo[columnName]
	mmp, err := mmap.MapRegion(f, PAGESIZE, mmap.RDWR, 0, 0)
	common.Check(err)
	return &mmp
}

func GetMMapRegion(nodeId int64, f os.File)(int64,*mmap.MMap){
	numberMmap := nodeId/MMAPSIZE + 1
	off := numberMmap * 1000 * 4096
	mmp, err := mmap.MapRegion(f, PAGESIZE, mmap.RDWR, 0, off)
	common.Check(err)
	return numberMmap,&mmp
}

func (metaPage MetaPage) ToBytes(){
}


func BytesToMetaPage(barr *[]byte)*MetaPage  {
	iStart, iEnd := 0, 0
	item := new(MetaPage)
	iEnd = iStart + common.INT64_LEN
	item.RootId = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.EmptyPageCount = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.BTreePageCount = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	item.EmptyPage = make([]byte,0, item.EmptyPageCount + item.BTreePageCount)
	copy(item.EmptyPage,(*barr)[iStart:iEnd])
	crc_0 := common.Crc16((*barr)[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item
}

func GETMetaPage(columnName string)*MetaPage {
	f := TableFileIo[columnName]
	mmp, err := mmap.MapRegion(f, 0, mmap.RDWR, 0, METAPAGEMAXLENGTH)
	common.Check(err)
	return BytesToMetaPage(mmp)
}

func BuildBTreeFromPage(baseTable string)*BTree{
	tr := New(DEGREE)
	metaPage := GETMetaPage(baseTable)
	rootId := metaPage.RootId
	baseTableFileIo := TableFileIo[baseTable]
	GetMMapRegion(rootId,baseTableFileIo)
	return tr
}