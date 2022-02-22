package main

import (
	"KeyDataStorage/Application/DataStorageEngine"
	"fmt"
)

func main() {
	DSE := DataStorageEngine.DataStorageEngine{}
	DSE.Init()

	fmt.Println(DSE.GET("kohahnj8gggdcb"))
	DSE.SET("as8gggd",make([]byte,15))
	DSE.SET("koffnj8gggdz",make([]byte,15))
	DSE.SET("koggasdgnj8gggdsz",make([]byte,15))
	DSE.SET("konhhahcj8gggdsg",make([]byte,15))
	DSE.SET("kobxcbnj8gggdb",make([]byte,15))
	DSE.SET("kohahnj8gggdcb",make([]byte,15))
	DSE.SET("konvcnnj8gggdxn",make([]byte,15))
	DSE.SET("kahdfhadfonj8gggdnbmcx",make([]byte,15))
	DSE.SET("konxcvnnj8gggdxmm",make([]byte,15))
	DSE.SET("koadfhnj8gggdmx",make([]byte,15))
	DSE.SET("koxcvnxcvnnj8gggdxcbm",make([]byte,15))

	fmt.Println(DSE.GET("kohahnj8gggdcb"))



}
