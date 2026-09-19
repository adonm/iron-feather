// Package tiles ports src/tiles.rs: XYZ MVT extension math + SQL.
package tiles

import (
	"fmt"
	"math"

	"github.com/adonm/lakewing/internal/filter"
)

// XYZToBBox converts a tile to a CRS84 bbox [w,s,e,n].
func XYZToBBox(z uint8, x, y uint32) [4]float64 {
	n := float64(uint64(1) << z)
	west := float64(x)/n*360 - 180
	east := float64(x+1)/n*360 - 180
	north := mercatorToLat(math.Pi * (1 - 2*float64(y)/n))
	south := mercatorToLat(math.Pi * (1 - 2*float64(y+1)/n))
	return [4]float64{west, south, east, north}
}

func mercatorToLat(m float64) float64 {
	return 180 / math.Pi * (2*math.Atan(math.Exp(m)) - math.Pi/2)
}

// ValidateTile mirrors the XYZ matrix check in tiles::tile.
func ValidateTile(z uint8, x, y uint32) error {
	if z > 30 || x >= (uint32(1)<<z) || y >= (uint32(1)<<z) {
		return fmt.Errorf("tile coordinates outside XYZ matrix")
	}
	return nil
}

// MVTSQL mirrors the single-scan MVT query in src/tiles.rs.
func MVTSQL(collection, from, fetch string, west, south, east, north float64) string {
	return fmt.Sprintf(
		"SELECT ST_AsMVT(t, %s) FROM (SELECT id, ST_AsMVTGeom("+
			"ST_Transform(page.geom, 'EPSG:4326', 'EPSG:3857', always_xy := true), "+
			"ST_Extent(ST_MakeEnvelope(%v, %v, %v, %v)), 4096, 64, true) AS geom "+
			"FROM (SELECT id, geom FROM %s WHERE %s ORDER BY id LIMIT 5000) AS page) t "+
			"WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) HAVING count(*) > 0",
		filter.Quote(collection), west, south, east, north, from, fetch)
}
