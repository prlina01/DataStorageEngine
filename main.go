package main

import (
	"KeyDataStorage/Application/DataStorageEngine"
	"fmt"
)

func main() {
	DSE := DataStorageEngine.DataStorageEngine{}.Init()
	fmt.Println("********* DATA STORAGE ENGINE *********")
	for{
		fmt.Println("1 - PUT")
		fmt.Println("2 - GET")
		fmt.Println("3 - DELETE")
		fmt.Println("4 - PUTHLL")
		fmt.Println("5 - PUTCMS")
		fmt.Println("6 - Exit")
		fmt.Print("Unesite jednu od ponudjenih opcija: ")
		var answer string
		var key string
		_, _ = fmt.Scanln(&answer)
		if answer == "1"{
			var value string
			fmt.Print("Unesite kljuc:")
			_, _ = fmt.Scanln(&key)
			fmt.Print("Unesite value:")
			_, _ = fmt.Scanln(&value)
			DSE.SET(key,[]byte(value))
			continue
		}else if answer == "2"{
			fmt.Print("Unesite kljuc, -hll ili -cms za te strukture:")
			_, _ = fmt.Scanln(&key)
			value := DSE.GET(key)
			if value == nil{
				fmt.Println("No such key")
			}else {
				fmt.Println(value)
			}
			continue
		}else if answer == "3"{
			fmt.Print("Unesite kljuc:")
			_, _ = fmt.Scanln(&key)
			DSE.DELETE(key)
		}else if answer == "4"{
			fmt.Print("Unesite kljuc:")
			_, _ = fmt.Scanln(&key)
			var keyhll string
			keyhll = key+"-hll"
			var data []string
			var element string
			fmt.Print("Unesite podatke za hll,x za kraj:")
			for{
				_,_ = fmt.Scanln(&element)
				if element == "x"{
					break
				}
				data = append(data,element)
			}
			if len(data) == 0 {
				continue
			}
			DSE.PUTHLL(keyhll,data)
			continue
		}else if answer == "5"{
			fmt.Print("Unesite kljuc:")
			_, _ = fmt.Scanln(&key)
			var keyhll string
			keyhll = key+"-cms"
			var data []string
			var element string
			fmt.Print("Unesite podatke za cms,x za kraj:")
			for{
				_,_ = fmt.Scanln(&element)
				if element == "x"{
					break
				}
				data = append(data,element)
			}
			if len(data) == 0 {
				continue
			}
			DSE.PUTCMS(keyhll,data)
			continue

		}else if answer == "6"{
			fmt.Println("10+ :D")
			break
		}else{
			fmt.Println("Greska! Pogresan unos")
			continue
		}

	}




}
