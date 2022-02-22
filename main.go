package main

import (
	"KeyDataStorage/Application/DataStorageEngine"
	"fmt"
)

func main() {
	DSE := DataStorageEngine.DataStorageEngine{}.Init()

	fmt.Println(DSE.GET("kohahnj8gggdcb"))
	fmt.Println(DSE.GET("koggasdgnj8gggdszv"))
	DSE.SET("as8gggdb",make([]byte,15))
	DSE.SET("koffnj8gggdzzgg",make([]byte,15))
	DSE.GET("koffnj8gggdzz")
	DSE.SET("koggasdgnj8gggdszv",make([]byte,15))
	DSE.SET("konhhahcj8gggdsgg",make([]byte,15))
	DSE.SET("kobxcbnj8gggdbh",make([]byte,15))
	DSE.SET("kohahnj8gggdcbs",make([]byte,15))
	DSE.SET("konvcnnj8gggdxnf",make([]byte,15))
	DSE.SET("kahdfhadfonj8gggdnbmcxh",make([]byte,15))
	DSE.SET("konxcvnnj8gggdxmmj",make([]byte,15))
	DSE.SET("koadfhnj8gggdmxy",make([]byte,15))
	DSE.SET("koxcvnxcvnnj8gggdxcbmt",make([]byte,15))



}
