package main

import (
	//"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net"
	"os"
	"strings"
)

type Punjab struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//punjab end

type Sindh struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//Sindh end
type Blochistan struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

type KPK struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//KPK end

type ICT struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

// ICT end
type GBT struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//GBT end
type AJK struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//AJK end
type KPTD struct {
	CTPerformed, CTPositive, Dischrg, Exp, Admtd, Region string
}

//KPTD Setters

//KPTD end

type Date struct {
	date       string
	punjab     *Punjab
	sindh      *Sindh
	blochistan *Blochistan
	ict        *ICT
	gbt        *GBT
	kpk        *KPK
	ajk        *AJK
	kptd       *KPTD
}

//Date setters
func setDate(str string, yeDate *Date) {
	yeDate.date = str
}
func setPunjab(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &Punjab{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.punjab = temp
}
func setSindh(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &Sindh{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.sindh = temp
}
func setBlochistan(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &Blochistan{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.blochistan = temp
}
func setKpk(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &KPK{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.kpk = temp
}

func setIct(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &ICT{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.ict = temp
}

func setGbt(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &GBT{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.gbt = temp
}

func setAjk(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &AJK{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd}
	yeDate.ajk = temp
}

func setKptd(ctperformed, ctpositive, dischrg, exp, admtd, region string, yeDate *Date) {
	temp := &KPTD{CTPerformed: ctperformed, CTPositive: ctpositive, Dischrg: dischrg, Exp: exp, Admtd: admtd, Region: region}
	yeDate.kptd = temp
}

//Date getters
func getDate(yeDate *Date) string {
	return yeDate.date
}

func getPunjab(yeDate *Date) (string, string, string, string, string, string) {

	return yeDate.punjab.CTPerformed, yeDate.punjab.CTPositive, yeDate.punjab.Dischrg, yeDate.punjab.Exp, yeDate.punjab.Admtd, yeDate.punjab.Region

}
func getSindh(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.sindh.CTPerformed, yeDate.sindh.CTPositive, yeDate.sindh.Dischrg, yeDate.sindh.Exp, yeDate.sindh.Admtd, yeDate.sindh.Region
}

func getBlochistan(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.blochistan.CTPerformed, yeDate.blochistan.CTPositive, yeDate.blochistan.Dischrg, yeDate.blochistan.Exp, yeDate.blochistan.Admtd, yeDate.blochistan.Region
}
func getKpk(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.kpk.CTPerformed, yeDate.kpk.CTPositive, yeDate.kpk.Dischrg, yeDate.kpk.Exp, yeDate.kpk.Admtd, yeDate.kpk.Region
}
func getIct(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.ict.CTPerformed, yeDate.ict.CTPositive, yeDate.ict.Dischrg, yeDate.ict.Exp, yeDate.ict.Admtd, yeDate.ict.Region
}

func getGbt(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.gbt.CTPerformed, yeDate.gbt.CTPositive, yeDate.gbt.Dischrg, yeDate.gbt.Exp, yeDate.gbt.Admtd, yeDate.gbt.Region
}
func getAjk(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.ajk.CTPerformed, yeDate.ajk.CTPositive, yeDate.ajk.Dischrg, yeDate.ajk.Exp, yeDate.ajk.Admtd, yeDate.ajk.Region
}
func getKptd(yeDate *Date) (string, string, string, string, string, string) {
	return yeDate.kptd.CTPerformed, yeDate.kptd.CTPositive, yeDate.kptd.Dischrg, yeDate.kptd.Exp, yeDate.kptd.Admtd, yeDate.kptd.Region
}

type Query struct {
	Date   string
	Region string
}

type myQuery struct {
	Query Query
}

//global date
var HashTable [139]Date

func main() {
	LoadData()
	var addr string
	var network string
	flag.StringVar(&addr, "e", ":4040", "service endpoint [ip addr or socket path]")
	flag.StringVar(&network, "n", "tcp", "network protocol [tcp,unix]")
	flag.Parse()
	//validate supported network protocol
	switch network {
	case "tcp", "tcp4", "tcp6", "unix":
	default:
		log.Fatalln("unsupported network protocol: ", network)
	}
	//create a listner
	ln, err := net.Listen(network, addr)
	if err != nil {
		log.Fatalln("failed to create listner: ", err)
	}
	defer ln.Close()
	log.Println("Service started: (%s) %s/n", network, addr)

	//connection loop, handlind the connection
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			if err := conn.Close(); err != nil {
				log.Println("failed to close listner: ", err)
			}
			continue
		}
		log.Println("connected to ", conn.RemoteAddr())

		go handleConnection(conn)
	}
}

//handle connection
func handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("error closing connection: ", err)
		}
	}()
	if _, err := conn.Write([]byte("Connected..\nUsage:Get <date,region>\n")); err != nil {
		log.Println("error waiting:", err)
		return
	}

	//loop to stay connected
	for {
		//buffer for client command
		cmdLine := make([]byte, (1024 * 4))
		n, err := conn.Read(cmdLine)
		if n == 0 || err != nil {
			log.Println("connection read error: ", err)
			return
		}
		//charstr := string(cmdLine)
		cmdLine1 := string(cmdLine[0:n])
		var query myQuery
		json.Unmarshal([]byte(cmdLine1), &query)
		//cmd, param := parseCommand(string(cmdLine[0:n]))
		if query.Query.Date != "" {
			//if query.query.date == "" {
			//	if _, err := conn.Write([]byte("invalid command")); err != nil {
			//		log.Println("failed to write:", err)
			//		return
			//	}
			//	continue
			//}
			//execute command
			//switch strings.ToUpper(query.query) {
			//case "query":
			//if string(param) == "date" {
			obj := DataAtDate(string(query.Query.Date))
			resjson1 := &HashTable[obj].punjab
			res1, _ := json.Marshal(resjson1)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson2 := &HashTable[obj].sindh
			res1, _ = json.Marshal(resjson2)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson3 := &HashTable[obj].kpk
			res1, _ = json.Marshal(resjson3)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson4 := &HashTable[obj].ict
			res1, _ = json.Marshal(resjson4)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson6 := &HashTable[obj].kptd
			res1, _ = json.Marshal(resjson6)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson7 := &HashTable[obj].ajk
			res1, _ = json.Marshal(resjson7)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}
			resjson8 := &HashTable[obj].gbt
			res1, _ = json.Marshal(resjson8)
			if _, err := conn.Write([]byte(fmt.Sprintf("%s\n", string(res1)))); err != nil {
				log.Println("failed to write: ", err)
			}

			//}
			//if _, err := conn.Write([]byte(param)); err != nil {
			//	log.Println("failed to write: ", err)
			//}
			continue
		} else {
			continue
		}
		//default:
		//	if _, err := conn.Write([]byte("invalid command\n")); err != nil {
		//		log.Println("failed to write: ", err)
		//		return
		//	}
		//}
	}
}

//parse command
func parseCommand(cmdLine string) (cmd, param string) {
	parts := strings.Split(cmdLine, " ")
	if len(parts) != 2 {
		return "", ""
	}
	cmd = strings.TrimSpace(parts[0])
	param = strings.TrimSpace(parts[1])
	return
}

func LoadData() {
	// Open the file
	lines, err := ReadCsv("covid_final_data.csv")
	if err != nil {
		panic(err)
	}
	//if lines[10][5] == "Punjab" {
	//	fmt.Print(lines[0][2])
	//} else {
	//	fmt.Println(len(lines[10][5]))
	//}
	PopulateData(lines)
}

func PopulateData(lines [][]string) {
	var hashcode, index int
	for i := 1; i < len(lines); i++ {
		mydata := lines[i]
		if i == 1 {
			hashcode = int(getHashcode(mydata[2]))
			index = getHashInd(hashcode, 139)
			//fmt.Println(index)
		} else if lines[i][2] != lines[i-1][2] {
			hashcode = int(getHashcode(mydata[2]))
			index = getHashInd(hashcode, 139)
			//fmt.Println(i)
		}
		setDate(mydata[2], &HashTable[index])
		if mydata[5] == "ICT" {
			//fmt.Println("yechz")
			setIct(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "ICT", &HashTable[index])
		} else if mydata[5] == "Punjab" {
			//fmt.Println("yechz", index)
			setPunjab(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "Punjab", &HashTable[index])
		} else if mydata[5] == "Sindh" {
			setSindh(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "Sindh", &HashTable[index])
			//fmt.Println("yechz", index)
		} else if mydata[5] == "KP" {
			setKpk(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "KP", &HashTable[index])
		} else if mydata[5] == "KPTD" {
			setKptd(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "KPTD", &HashTable[index])
		} else if mydata[5] == "Balochistan" {
			setBlochistan(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "Balochistan", &HashTable[index])
		} else if mydata[5] == "AJK" {

			setAjk(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "AJK", &HashTable[index])
		} else if mydata[5] == "GB" {
			//fmt.Println("ye chz")
			setGbt(mydata[1], mydata[0], mydata[3], mydata[4], mydata[6], "GB", &HashTable[index])
		}
		//fmt.Println(HashTable[index].punjab.CTPositive)
	}
}

func getHashcode(date string) uint32 {
	s := []rune(date)
	shortstr := string(s[0:5])
	//fmt.Println(shortstr)
	h := fnv.New32a()
	h.Write([]byte(shortstr))
	return h.Sum32()
}

func getHashInd(hashkey, blocksize int) int {
	var slot int = 0
	for i := 0; true; i++ {
		slot = int(math.Mod(float64((hashkey + i*i)), float64(blocksize)))
		if HashTable[slot].date == "" {
			break
		} else {
			continue
		}
	}
	return slot
}
func DataAtDate(date string) int {
	hashkey := int(getHashcode(date))
	index := searchByDate(hashkey, 139, date)
	return index
}

func searchByDate(hashkey, blocksize int, date string) int {
	var slot int = 0
	for i := 0; true; i++ {
		slot = int(math.Mod(float64((hashkey + i*i)), float64(blocksize)))
		if HashTable[slot].date == date {
			break
		} else {
			continue
		}
	}
	return slot

}

func ReadCsv(filename string) ([][]string, error) {

	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		return [][]string{}, err
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return [][]string{}, err
	}

	return lines, nil
}
