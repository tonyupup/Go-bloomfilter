package filter

import (
	"errors"
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

//BloomFilter interface
type BloomFilter interface {
	hash([]byte) []uint64 //hash funcreturn uint64 slices
	setStroge(BloomFilterStorger, uint64) error
	Exist([]byte) bool
	Add([]byte) error
	Count() uint64
	Close() error
}

type BloomFiltertor struct {
	hashFuncWithSeed []hash.Hash64
	totalBit         uint64
	stroge           BloomFilterStorger
}

func NewBloomFilter(stroge BloomFilterStorger, errorRatio float64, totalCount uint64) (BloomFilter, error) {
	seeds := []uint32{
		543, 460, 171, 876, 796, 607, 650, 81, 837, 545, 591, 946, 846, 521, 913, 636, 878, 735, 414, 372,
		344, 324, 223, 180, 327, 891, 798, 933, 493, 293, 836, 10, 6, 544, 924, 849, 438, 41, 862, 648, 338,
		465, 562, 693, 979, 52, 763, 103, 387, 374, 349, 94, 384, 680, 574, 480, 307, 580, 71, 535, 300, 53,
		481, 519, 644, 219, 686, 236, 424, 326, 244, 212, 909, 202, 951, 56, 812, 901, 926, 250, 507, 739, 371,
		63, 584, 154, 7, 284, 617, 332, 472, 140, 605, 262, 355, 526, 647, 923, 199, 518}
	m := math.Ceil(-float64(totalCount) * math.Log(errorRatio) / math.Pow(math.Ln2, 2)) // 需要的总bit位数
	// 优化内存 m=2^n
	m1 := uint64(1 << uint32(math.Ceil(math.Log2(m))))
	// total := uint64(math.Ceil(float64(-m1) * math.Pow(math.Ln2, 2) / math.Log(errorRetio)))

	k := int(math.Ceil(-math.Log2(errorRatio)))
	seedFunc := make([]hash.Hash64, k)
	for index, sed := range seeds[:k] {
		seedFunc[index] = murmur3.New64WithSeed(sed)
	}
	filter := &BloomFiltertor{hashFuncWithSeed: seedFunc, totalBit: m1, stroge: stroge}
	if err := filter.setStroge(stroge, m1); err != nil {
		return nil, err
	}
	return filter, nil
}
func (bf *BloomFiltertor) setStroge(stroge BloomFilterStorger, totalCount uint64) error {
	return stroge.new(totalCount)
}
func (bf *BloomFiltertor) Add(data []byte) error {
	if len(data) == 0 {
		return errors.New("no content")
	}
	return bf.stroge.add(bf.hash(data))
}

func (bf *BloomFiltertor) Count() uint64 {
	return bf.stroge.count()
}

func (bf *BloomFiltertor) Exist(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	return bf.stroge.exist(bf.hash(data))
}

func (bf *BloomFiltertor) hash(data []byte) []uint64 {
	hashResult := make([]uint64, len(bf.hashFuncWithSeed))
	for index, fun := range bf.hashFuncWithSeed {
		fun.Write(data)
		// defer fun.Reset()
		hashResult[index] = fun.Sum64() & (bf.totalBit - 1)
		fun.Reset()
	}
	return hashResult
}

func (bf *BloomFiltertor) Close() error {
	return bf.stroge.close()
}
