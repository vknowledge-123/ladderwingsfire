import asyncio
import json
import logging
import os
import queue
import threading
import time
import hashlib
from collections import Counter
from config import StrategySettings, StockStatus
from dhan_client import DhanClientWrapper
from order_manager import OrderManager
from performance_monitor import perf_monitor
import pandas as pd
import numpy as np
from typing import Dict, List, Iterable, Tuple, Optional, Any
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from redis_store import load_candidates

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

def _stock_list_signature(symbols: Iterable[str]) -> Tuple[int, str]:
    normalized = [str(s).strip().upper() for s in symbols if str(s).strip()]
    unique_sorted = sorted(set(normalized))
    payload = "\n".join(unique_sorted).encode("utf-8")
    return len(unique_sorted), hashlib.sha256(payload).hexdigest()


STOCK_LIST = [
  "MRF","3MINDIA","HONAUT","ABBOTINDIA","JSWHL","POWERINDIA","PTCIL","FORCEMOT","NEULANDLAB","LMW",
  "TVSHLTD","MAHSCOOTER","ZFCVINDIA","PGHH","BAJAJHLDNG","DYNAMATECH","ASTRAZEN","APARINDS","GILLETTE",
  "WENDT","TASTYBITE","VOLTAMP","CRAFTSMAN","NSIL","ESABINDIA","NAVINFLUOR","ICRA","LINDEINDIA","ATUL",
  "VSTTILLERS","JKCEMENT","PGHL","LUMAXIND","BLUEDART","CERA","PILANIINVS","FOSECOIND","VADILALIND","KICL",
  "PFIZER","SUNDARMFIN","ORISSAMINE","LTTS","SANOFICONR","CRISIL","ECLERX","FINEORG","BAYERCROP",
  "TVSSRICHAK","SANOFI","SHILCTECH","BASF","KINGFA","SCHAEFFLER","SMLMAH","AIAENG","CEATLTD","SWARAJENG",
  "GRWRHITECH","ESCORTS","BANARISUG","AKZOINDIA","INGERRAND","VHL","FLUOROCHEM","KIRLOSIND","KMEW","RADICO",
  "NETWEB","THANGAMAYL","SHRIPISTON","PRIVISCL","TIMKEN","GVT&D","ETHOSLTD","TCPLPACK","WAAREEENER",
  "ANANDRATHI","NDGL","ENRIN","LALPATHLAB","THERMAX","GODFRYPHLP","BBL","CARTRADE","AJANTPHARM",
  "BHARATRAS","ENDURANCE","PRUDENT","AIIL","GLAXO","DATAPATTNS","DOMS","RATNAMANI","SHAILY","INTERARCH",
  "GRSE","HONDAPOWER","BALKRISIND","MTARTECH","HYUNDAI","JUBLCPL","COROMANDEL","RPGLIFE","KDDL","CENTUM",
  "FIEMIND","SAFARI","NBIFIN","POWERMECH","INDIAMART","V2RETAIL","STYLAMIND","ANUP","TIIL","MASTEK","E2E",
  "STYRENIX","MPSLTD","GALAXYSURF","SUMMITSEC","CAPLIPOINT","CHOLAHLDNG","TEGA","METROPOLIS",
  "LGBBROSLTD","POLYMED","AUTOAXLES","NH","BBTC","GRAVITA","SKFINDIA","CIGNITITEC","TATACOMM",
  "JBCHEPHARM","GRPLTD","ACC","GKWLIMITED","SANSERA","BEML","AFFLE","BHARTIHEXA","GLAND","ABREL","ZOTA",
  "SJS","ACUTAAS","TBOTEK","UBL","IKS","MAPMYINDIA","BETA","KIRLOSBROS","VEEDOL","ONESOURCE","IFBIND",
  "AZAD","HESTERBIO","TEAMLEASE","COCHINSHIP","INDOTECH","THEJO","ALKYLAMINE","VINATIORGA","PGIL",
  "GRINDWELL","YASHO","ERIS","LGEINDIA","AAVAS","BIRLANU","CPPLUS","CARERATING","EIMCOELECO","DEEPAKNTR",
  "JINDALPHOT","PIRAMALFIN","SOLEX","HIRECT","ARMANFIN","LUMAXTECH","IPCALAB","MIDWESTLTD","PIXTRANS",
  "NPST","SOBHA","EMCURE","TATVA","DPABHUSHAN","WELINV","JCHAC","RRKABEL","VENKEYS","NILKAMAL","JLHL",
  "VINDHYATEL","EPIGRAL","IMFA","ZENTEC","RAINBOW","CONCORDBIO","RANEHOLDIN","MANORAMA","WOCKPHARMA",
  "NGLFINE","ACCELYA","ANURAS","POCL","CREDITACC","LLOYDSME","TRAVELFOOD","AMBIKCO","SUNCLAY",
  "PUNJABCHEM","IFBAGRO","VENUSPIPES","WABAG","DCMSHRIRAM","NESCO","DEEPAKFERT","INDIGOPNTS","SASKEN",
  "DODLA","SPECTRUM","OLECTRA","DHANUKA","APOLSINHOT","HOMEFIRST","KPIL","METROBRAND","DHUNINV",
  "GULFOILLUB","MALLCOM","MEDANTA","AURIONPRO","UTIAMC","KIRLOSENG","INOXINDIA","RAYMONDLSL","MGL",
  "SIGNATURE","BALAMINES","LUXIND","GESHIP","TECHNOE","NIBE","GOODLUCK","JPOLYINVST","NEOGEN","DIAMONDYD",
  "JUBLPHARMA","ADOR","GMMPFAUDLR","HAPPYFORGE","BIRLACORPN","INDIAGLYCO","SANDESH","TTKHLTCARE",
  "CHEVIOT","RAMCOCEM","KAJARIACER","PVRINOX","TCI","RACLGEAR","KIRLPNU","IMPAL","EIDPARRY","DREDGECORP",
  "INTELLECT","SEAMECLTD","KERNEX","GRINFRA","CCL","EXPLEOSOL","HATSUN","GODREJIND","MACPOWER","WEALTH",
  "GROBTEA","GMBREW","SUDARSCHEM","ENTERO","ASAHIINDIA","RPEL","AJMERA","VIJAYA","DSSL","KPRMILL","KSCL",
  "GABRIEL","XPROINDIA","GLOBUSSPR","BATAINDIA","ASALCBR","AHLUCONT","JYOTICNC","SHARDAMOTR","WAAREERTL",
  "MAITHANALL","UNIMECH","SUNDRMFAST","ELDEHSG","ACE","ARE&M","EXCELINDUS","CARYSIL","CHENNPETRO",
  "WHIRLPOOL","ATLANTAELE","NUCLEUS","PREMIERENE","SHARDACROP","CANFINHOME","CLEAN","ASTRAMICRO",
  "NATCOPHARM","KAUSHALYA","CHALET","SAILIFE","PSPPROJECT","UNIVCABLES","ALIVUS","BRIGADE","STAR","APLLTD",
  "CARBORUNIV","GANECOS","AVALON","NAM-INDIA","SYMPHONY","ALLDIGI","SUBROS","POKARNA","AETHER",
  "MOTILALOFS","INDIASHLTR","ALICON","NEWGEN","IZMO","ORCHPHARMA","YUKEN","GOKEX","ELECTHERM","CENTURYPLY",
  "WHEELS","PASHUPATI","CEMPRO","POLYPLEX","FACT","DATAMATICS","PITTIENG","RIIL","NDRAUTO","GANESHHOU",
  "HBLENGINE","ISGEC","AVANTIFEED","MEDPLUS","SHYAMMETL","SILVERTUC","INDNIPPON","TINNARUBR","WELCORP",
  "SENORES","WINDLAS","CNL","JKLAKSHMI","KRN","PROTEAN","ALBERTDAVD","HDBFS","PKTEA","ICEMAKE","HEXT",
  "63MOONS","VMART","BIL","NELCO","HGINFRA","DECCANCE","MANGLMCEM","GALAPREC","ABSLAMC","AEGISLOG",
  "GANDHITUBE","RPSGVENT","CHOICEIN","TEMBO","ASHAPURMIN","SUPRIYA","RML","AARTIPHARM","FINCABLES",
  "SYRMA","KSB","ZENSARTECH","KRSNAA","BIKAJI","SPAL","CONTROLPR","AGI","KSL","TATAINVEST","INNOVACAP",
  "SVLL","CAPILLARY","COSMOFIRST","SCHNEIDER","SUNDROP","HCG","RVTH","JUSTDIAL","BANCOINDIA","DENORA",
  "VENTIVE","JSLL","MONTECARLO","ASTEC","QPOWER","INSECTICID","YATHARTH","ANTHEM","SUDEEPPHRM",
  "SALZERELEC","JUBLINGREA","INFOBEAN","KEC","BUTTERFLY","AMRUTANJAN","TDPOWERSYS","AGARIND","FAIRCHEMOR",
  "ROUTE","SUNDRMBRAK","ROSSTECH","GARFIBRES","PARAS","KIMS","RATEGAIN","KALYANIFRG","RAMCOSYS","SPLPETRO",
  "BLACKBUCK","SHAKTIPUMP","VARROC","SIYSIL","EUREKAFORB","ATHERENERG","TTKPRESTIG","SWELECTES",
  "GLOSTERLTD","BALUFORGE","RUBICON","SUYOG","ABDL","ORKLAINDIA","STOVEKRAFT","EMUDHRA","ASTERDM",
  "RAMRAT","PNGJL","PRICOLLTD","AJAXENGG","VIMTALABS","ARSSBL","MANYAVAR","ARVSMART","WEWORK","DIVGIITTS",
  "GALLANTT","GODREJAGRO","SOLARA","ROSSARI","MINDACORP","TRANSRAILL","PAUSHAKLTD","SFL","GHCL",
  "MOLDTKPAC","CELLO","JBMA","FIVESTAR","TCIEXP","NAVA","JKIL","SRM","SUNTV","KIRIINDUS","SANDHAR",
  "MAHSEAMLES","BLUEJET","JARO","PICCADIL","TANLA","ITDC","RAMKY","WESTLIFE","ANANTRAJ","MARATHON",
  "MATRIMONY","TIPSMUSIC","BORORENEW","STUDDS","WONDERLA","CARRARO","GRAPHITE","BERGEPAINT","EMAMILTD",
  "RUSTOMJEE","OPTIEMUS","COHANCE","HEG","HNDFDS","TRITURBINE","SGIL","OSWALPUMPS","STEL","BLUESTONE",
  "INDGN","KRISHANA","ARROWGREEN","GMDCLTD","KRYSTAL","LANDMARK","BBOX","RKFORGE","SILINV","EVERESTIND",
  "WELENT","LEMERITE","SARDAEN","APOLLOTYRE","AVL","GUJALKALI","TMB","MAGADSUGAR","AGARWALEYE","JINDRILL",
  "PREMEXPLN","ASAL","VISHNU","LATENTVIEW","JINDALPOLY","AWFIS","ARVINDFASN","SRHHYPOLTD","GNFC",
  "HAPPSTMNDS","KKCL","ACI","UNIPARTS","AADHARHFC","DICIND","ELGIEQUIP","MAYURUNIQ","RAYMONDREL","ELECON",
  "MANORG","TEJASNET","VESUVIUS","BAJAJELEC","NRAIL","SIRCA","TENNIND","LINCOLN","HERITGFOOD","SMARTWORKS",
  "GOCOLORS","UFLEX","MSTCLTD","INDRAMEDCO","SHANTIGEAR","KRISHIVAL","MEDIASSIST","REPRO","ASKAUTOLTD",
  "HSCL","STARHEALTH","FMGOETZE","CHEMFAB","HLEGLAS","TSFINV","FAZE3Q","SUMICHEM","SWANCORP","UNICHEMLAB",
  "JKTYRE","TI","RAYMOND","SBCL","GRMOVER","SANATHAN","CENTENKA","HGS","VTL","DBL","RAJRATAN","JASH",
  "AWHCL","SUPRAJIT","MAXESTATES","INNOVANA","UNITEDTEA","MANINDS","SANGAMIND","HEUBACHIND","RHIM",
  "ATULAUTO","DEEPINDS","USHAMART","PRECOT","GUJAPOLLO","SHOPERSTOP","BALRAMCHIN","SKIPPER","SKMEGGPROD",
  "THYROCARE","JSFB","CHAMBLFERT","IGARASHI","BSOFT","CYIENTDLM","DLINKINDIA","ZYDUSWELL","IDEAFORGE",
  "NIPPOBATRY","KPIGREEN","AKUMS","SAKAR","MAMATA","HINDCOMPOS","SOMANYCERA","CEWATER","FDC","SWIGGY",
  "ABCOTS","INDIACEM","GENESYS","EMSLIMITED","BAJAJHCARE","INDIANHUME","BFINVEST","NINSYS","MBAPL",
  "WSTCSTPAPR","TIPSFILMS","TRUALT","IGPL","LENSKART","MEDICAMEQ","RELAXO","NIITMTS","RSYSTEMS",
  "REPCOHOME","MAHLIFE","SUNTECK","RISHABH","AARTISURF","CIEINDIA","AFCONS","THELEELA","ANTELOPUS","GUJGASLTD",
  "AARTIDRUGS","CUPID","MODINATUR","HPL","GOACARBON","CSBBANK","KRBL","GUJTHEM","BELLACASA","TAJGVK",
  "SGFIN","KOLTEPATIL","SHREEPUSHK","ROHLTD","JAINREC","AVADHSUGAR","GICRE","HINDCOPPER","LTFOODS","DOLPHIN",
  "EIHOTEL","MBEL","SAATVIKGL","SHIVALIK","TMCV","HARSHA","SOTL","APCOTEXIND","GOLDIAM","MEIL","JGCHEM",
  "SAREGAMA","JKPAPER","BESTAGRO","PANACEABIO","PDSL","CREST","MMFL","EIHAHOTELS","TRIVENI","AARTIIND",
  "HARIOMPIPE","KAYA","ELLEN","DOLLAR","VIPIND","IONEXCHANG","CMSINFO","SONATSOFTW","KALPATARU",
  "THOMASCOTT","GUFICBIO","ZAGGLE","KPEL","TMPV","M&MFIN","MODIS","NUVOCO","KIOCL","RPTECH","ORIENTTECH",
  "GODIGIT","KILITCH","FSL","GEECEE","SGMART","DYCL","SHREEJISPG","SHILPAMED","VGUARD","VIDHIING",
  "RAILTEL","ASIANHOTNR","MOIL","SIS","EVEREADY","TATACAP","SOFTTECH","MAHLOG","MASFIN","SKYGOLD","ARIES",
  "BAJAJINDEF","SMSPHARMA","BLS","IGIL","PARAGMILK","IIFLCAPS","BANSALWIRE","DENTA","ARVIND","NITINSPIN",
  "RAMCOIND","ZUARIIND","ARIHANTSUP","KEYFINSERV","MHRIL","DIFFNKG","GOPAL","PCBL","GVPIL","SENCO",
  "ADVENZYMES","SEMAC","5PAISA","ZODIAC","SANGHVIMOV","ITI","IRIS","MONARCH","GNA","PRAJIND","GENUSPOWER",
  "GOCLCORP","TRF","EUROPRATIK","SASTASUNDR","MIDHANI","APOLLOPIPE","SHIVAUM","VLSFINANCE","BOROLTD",
  "FLAIR","CRAMC","KAPSTON","DALMIASUG","NURECA","ASIANENE","EPACKPEB","STYL","EFCIL","ZUARI","APTUS",
  "ASHIANA","CSLFINANCE","DVL","FIRSTCRY","GSPL","VSSL","KSOLVES","SOLARWORLD","ICIL","IVALUE","SALONA",
  "NRBBEARING","S&SPOWER","OAL","SCPL","DDEVPLSTIK","IRMENERGY","JYOTHYLAB","PONNIERODE","SURAKSHA",
  "CRIZAC","QUICKHEAL","TALBROAUTO","PRIMESECU","REDINGTON","TARIL","ARTEMISMED","TEAMGTY","LIBERTSHOE",
  "EBGNG","MUTHOOTCAP","ONWARDTEC","WINDMACHIN","PANAMAPET","RITCO","GREENPLY","STYLEBAAZA","HINDWAREAP",
  "INDOBORAX","VALIANTORG","JSWINFRA","CUB","SIMPLEXINF","DYNPRO","BECTORFOOD","SPANDANA","CPEDU",
  "STERTOOLS","STARTECK","MWL","VRLLOG","ORIENTBELL","FINOPB","SRGHFL","QUADFUTURE","ALLTIME","LAXMIDENTL",
  "TIL","LGHL","APEX","AGIIL","SURAJEST","RALLIS","CAMPUS","CAPITALSFB","NAHARCAP","DIAMINESQ","HONASA",
  "CHEMPLASTS","JUNIPER","SURYAROSNI","DMCC","JWL","SURAJLTD","NORTHARC","NAHARPOLY","CANTABIL","CAPACITE",
  "CLSEL","IXIGO","EPACK","VSTIND","PODDARMENT","REFEX","PNCINFRA","BAJAJCON","UTTAMSUGAR","GODAVARIB",
  "HIKAL","PRINCEPIPE","GREENLAM","SUNFLAG","MMP","PPL","DELPHIFX","ASAHISONG","SAHYADRI","AEGISVOPAK",
  "AKSHARCHEM","AWL","PLATIND","KARURVYSYA","HUBTOWN","ZENITHEXPO","NLCINDIA","PURVA","VIKRAMSOLR",
  "INDIANCARD","INDOCO","INDOSTAR","DCI","DBCORP","SESHAPAPER","HERANBA","UNIVPHOTO","VINYLINDIA",
  "GANESHCP","FABTECH","GPIL","SAPPHIRE","GREENPANEL","DHARMAJ","MOBIKWIK","WANBURY","PINELABS","ASPINWALL",
  "VAIBHAVGBL","APOLLO","PRECWIRE","CEIGALL","JNKINDIA","RELIGARE","TIRUMALCHM","KAMATHOTEL","ECOSMOBLTY",
  "EIFFL","BHAGCHEM","TARSONS","ACMESOLAR","RITES","NAZARA","SCI","MHLXMIRU","STANLEY","KANSAINER",
  "ADVENTHTL","SIGNPOST","STARCEMENT","MAZDA","ALPHAGEO","KABRAEXTRU","DCAL","SREEL","PPAP","SWSOLAR",
  "NIRAJISPAT","RUBYMILLS","DEEDEV","BLSE","EPL","SANDUMA","SULA","LOYALTEX","SAGCEM","HUHTAMAKI",
  "DAMCAPITAL","STEELCAS","ADFFOODS","UNIDT","AFFORDABLE","SEQUENT","QUESS","IFGLEXPOR","PATELRMART",
  "JAYKAY","MOSCHIP","KNAGRI","LAOPALA","BLAL","UTLSOLAR","JAYAGROGN","ARVEE","INOXGREEN","MARINE","GARUDA",
  "JAGSNPHARM","KTKBANK","CHEMCON","AFSL","PENIND","PFOCUS","EIEL","RHL","PACEDIGITK","NAHARSPING",
  "KANPRPLA","GLOBALVECT","NCLIND","BBTCL","MINDTECK","NITIRAJ","SHRINGARMS","DPWIRES","GOKULAGRO","KITEX",
  "RESPONIND","INDIQUBE","SHANTIGOLD","MARKSANS","EMMVEE","ITCHOTELS","SHALBY","ENGINERSIN","RAJESHEXPO",
  "MAXIND","INDOFARM","20MICRONS","SAKSOFT","VERANDA","NATCAPSUQ","SSWL","GPPL","TIMETECHNO","PRABHA",
  "CORDSCABLE","ORBTEXP","LIKHITHA","CGCL","CASTROLIND","AARON","MVGJL","UFBL","GREAVESCOT","EUROBOND",
  "SHREYANIND","KCP","LFIC","TRANSWORLD","ORIENTELEC","SAMHI","PROSTARM","BHARATWIRE","BHARATSE",
  "BALMLAWRIE","KROSS","IKIO","BHAGERIA","YATRA","AEROFLEX","SHIVATEX","WALCHANNAG","HEIDELBERG",
  "MUTHOOTMF","AURUM","AMNPLST","MANCREDIT","LORDSCHLO","SPMLINFRA","LOKESHMACH","SHAREINDIA","UDS",
  "GSFC","NIACL","IITL","WEL","DCBBANK","KHADIM","UGROCAP","LXCHEM","SUVEN","ELIN","MARKOLINES","IPL",
  "HITECHCORP","PYRAMID","SHK","HEXATRADEX","BAJEL","CONSOFINVT","FINPIPE","ORIENTCEM","AVG","RELTD",
  "ASHOKA","VINCOFE","MEESHO","URAVIDEF","CHEMBOND","TBZ","DCMSRIND","CAMLINFINE","PLASTIBLEN",
  "LEMONTREE","RUPA","BSL","DCXINDIA","DTIL","GICHSGFIN","HARRMALAYA","AYMSYNTEX","JINDALSAW","STARPAPER",
  "SCHAND","CHEMBONDCH","MOLDTECH","SYSTMTXC","TAINWALCHM","BELRISE","RATNAVEER","NOCIL","FUSION",
  "HISARMETAL","HERCULES","GKENERGY","ADSL","SCODATUBES","IRCON","SGLTL","LOTUSDEV","RAMAPHO","MAANALU",
  "PARADEEP","PTC","PRECAM","WIPL","RSWM","NATHBIOGEN","AVANTEL","HINDOILEXP","KALAMANDIR","VGL",
  "SAMMAANCAP","MRPL","RACE","BIRLAMONEY","SUKHJITS","GIPCL","IVP","SUPERHOUSE","AEQUS","JMFINANCIL",
  "TARC","KNRCON","GROWW","JOCIL","ADANIPOWER","BLISSGVS","SATIN","JTEKTINDIA","ARKADE","TNPL","FEDFINA",
  "GANGESSECU","KRONOX","WORTHPERI","GEMAROMA","NAVNETEDUL","RBZJEWEL","DIACABS","MODISONLTD","DIGITIDE",
  "GPTHEALTH","THOMASCOOK","MANBA","RCF","RELCHEMQ","CROWN","WELSPUNLIV","UNIENTER","SURYODAY",
  "MUKANDLTD","SPARC","GULPOLY","MONEYBOXX","PWL","MANAKCOAT","TVTODAY","VMM","PRAKASH","HEMIPROP",
  "DEVYANI","PIGL","TOLINS","AHLEAST","KAKATCEM","BIRLACABLE","ROLEXRINGS","SMARTLINK","SOMICONVEY","HPIL",
  "PARKHOTELS","LAXMIINDIA","BOMDYEING","MANINFRA","DHAMPURSUG","KOPRAN","PRSMJOHNSN","RGL","BANARBEADS",
  "JAICORPLTD","AGRITECH","LOTUSEYE","DCMNVL","URBANCO","HECPROJECT","ABLBL","INDOUS","VSTL","MAHEPC",
  "KOTHARIPET","TEXRAIL","BOROSCI","ARISINFRA","JAMNAAUTO","GANDHAR","LAMBODHARA","MUNJALSHOW","REDTAPE",
  "PVSL","VRAJ","APCL","STCINDIA","AARVI","CANHLIFE","RNBDENIMS","ADVANCE","WCIL","JSWCEMENT","REPL",
  "RUCHIRA","MASTERTR","DBREALTY","UNIECOM","DBEIL","EKC","INDOAMIN","RICOAUTO","REMSONSIND","GAEL",
  "SECMARK","HIMATSEIDE","SPECIALITY","THEINVEST","UCAL","GMRP&UI","ENIL","AVROIND","CPCAP","BANSWRAS",
  "RKSWAMY","SHANKARA","TOKYOPLAST","LINC","DREAMFOLKS","EXICOM","MUFIN","EMIL","TVSSCS","NELCAST",
  "AUSOMENT","NAHARINDUS","SHEMAROO","PALASHSECU","BALAJITELE","JKIPL","VETO","SDBL","ESTER","RAIN",
  "GILLANDERS","ORIENTHOT","MENONBE","TEXINFRA","SINTERCOM","AMANTA","SBFC","PAKKA","TNPETRO","TOUCHWOOD",
  "AIROLAM","EDELWEISS","GPTINFRA","THEMISMED","MODIRUBBER","OMINFRAL","J&KBANK","RPPINFRA","ALEMBICLTD",
  "KECL","BEDMUTHA","STLTECH","GTPL","WEIZMANIND","FINKURVE","IDBI","ISFT","OMAXAUTO","DONEAR",
  "PDMJEPAPER","EXCELSOFT","BSHSL","KHAITANLTD","BROOKS","MUFTI","INDSWFTLAB","SMLT","APTECHT","STEELCITY",
  "EMMBI","BAJAJHFL","MAHAPEXLTD","CUBEXTUB","SAMBHV","OCCLLTD","NAVKARCORP","ARIHANTCAP","INTLCONV",
  "ZEEL","VIKRAN","SANSTAR","ATLASCYCLE","ARCHIDPLY","DCM","PAR","HITECH","NTPCGREEN","KUANTUM","NITCO",
  "KOKUYOCMLN","WEBELSOLAR","NDLVENTURE","SPORTKING","BEPL","EMAMIPAP","SHREDIGCEM","OMFREIGHT",
  "JAYBARMARU","INSPIRISYS","NIITLTD","JAYSREETEA","KRITI","ZODIACLOTH","AERONEU","SAURASHCEM","NFL",
  "DOLATALGO","EMAMIREAL","SARLAPOLY","CINELINE","SHRIRAMPPS","SMCGLOBAL","IGCL","MAWANASUG","WSI",
  "SINCLAIR","IOLCP","SERVOTECH","KHAICHEM","LOVABLE","CLEDUCATE","XCHANGING","NDTV","ALKALI","FOCUS",
  "ATAM","SUPREME","SSDL","SPIC","ALPA","TOTAL","AEROENTER","DBOL","RMDRIP","PNBGILTS","ANUHPHR",
  "MUNJALAU","GANESHBE","ADL","JMA","LYKALABS","UFO","ORIENTLTD","ANDHRSUGAR","KANORICHEM","TARACHAND",
  "NIVABUPA","VERTOZ","RUBFILA","AUTOIND","RADHIKAJWE","BALPHARMA","FOODSIN","ACL","ZIMLAB","BRIGHOTEL",
  "MADRASFERT","VPRPL","JAYNECOIND","KOTHARIPRO","EMBDL","GHCLTEXTIL","REGAAL","SILGO","DELTAMAGNT",
  "ELECTCAST","DELTACORP","JAGRAN","GEOJITFSL","TRIGYN","SUMIT","SHAHALLOYS","MITCON","KREBSBIO",
  "BLUSPRING","LAGNAM","RBA","ANDHRAPAP","SATIA","VALIANTLAB","AVTNPL","TPLPLASTEH","TFCILTD",
  "VISAKAIND","BIGBLOC","HMVL","UNIVASTU","ISHANCH","BANKA","NKIND","OILCOUNTUB","HGM","OMAXE",
  "KRITINUT","MOL","ASIANTILES","RAJOOENG","SANGHIIND","MANAKSIA","GLOBECIVIL","MUKTAARTS","ROTO",
  "SHIVAMILLS","HILINFRA","MANALIPETC","JTLIND","JAIBALAJI","DCW","NRL","GATEWAY","GLOTTIS","BMWVENTLTD",
  "SHALPAINTS","SRD","DJML","OSWALAGRO","GFLLIMITED","EQUITASBNK","VASWANI","SURYALAXMI","ADVANIHOTR",
  "LLOYDSENT","PRITI","RSSOFTWARE","MANAKSTEEL","BPL","MAHABANK","ONMOBILE","SIGIND","OBCL","MMTC",
  "ANIKINDS","RKEC","ROSSELLIND","VMSTMT","UJJIVANSFB","FILATEX","ELGIRUBCO","RADIANTCMS","BODALCHEM",
  "PASUPTAC","BYKE","GOLDTECH","LLOYDSENGG","ONEPOINT","KARMAENG","TARMAT","VIDYAWIRES","MEDICO",
  "BLKASHYAP","AMJLAND","AHLADA","AMDIND","ROML","TEXMOPIPES"
]
class LadderEngine:
    def __init__(self, dhan_client: DhanClientWrapper):
        self.dhan_client = dhan_client
        self.order_manager = OrderManager(dhan_client)
        self.settings = StrategySettings()
        self.active_stocks: Dict[str, StockStatus] = {}
        self.started_symbols = set()
        self.armed_for_market_open = False
        self.running = False
        self.pnl_global = 0.0
        self.trading_halted = False
        self.trading_halt_reason = ""

        # Reactive selection throttling (avoid sorting on every tick)
        self._last_select_ts = 0.0
        self._select_interval_seconds = 0.25

        # Optional mover-diagnostics throttle (file/log spam guard)
        self._last_movers_diag_ts = 0.0
        
        # Cache for performance
        self.filtered_stocks_cache = None
        self.cache_timestamp = None

        # Async order execution (never block tick thread)
        self._order_queue: "queue.Queue[dict]" = queue.Queue(maxsize=2000)
        self._order_stop = threading.Event()
        self._order_workers: list[threading.Thread] = []
        self._stock_locks: Dict[str, threading.RLock] = {}
        self._order_manager_lock = threading.Lock()
        self._started_lock = threading.Lock()
        self._pending_start_symbols: set[str] = set()
        self._order_generation = 0
        self._ensure_order_workers()
        
        # Pre-calculate multipliers
        self._update_multipliers()

    def _get_stock_lock(self, symbol: str) -> threading.RLock:
        lock = self._stock_locks.get(symbol)
        if lock is None:
            lock = threading.RLock()
            self._stock_locks[symbol] = lock
        return lock

    def _ensure_order_workers(self):
        """Ensure we have the right number of order worker threads."""
        desired = max(1, int(getattr(self.settings, "max_concurrent_orders", 2) or 2))
        live = [t for t in self._order_workers if t.is_alive()]
        self._order_workers = live

        if len(self._order_workers) == desired:
            return

        # If count changed, stop old workers and restart cleanly.
        if self._order_workers:
            self._order_stop.set()
            for _ in self._order_workers:
                try:
                    self._order_queue.put_nowait({"kind": "STOP"})
                except Exception:
                    pass
            self._order_workers = []
            self._order_stop = threading.Event()

        for i in range(desired):
            t = threading.Thread(
                target=self._order_worker_loop,
                name=f"order-worker-{i+1}",
                daemon=True,
            )
            t.start()
            self._order_workers.append(t)

    def _order_worker_loop(self):
        while not self._order_stop.is_set():
            try:
                task = self._order_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                if not isinstance(task, dict):
                    continue
                if task.get("kind") == "STOP":
                    return
                self._execute_order_task(task)
            except Exception as e:
                logger.error(f"Order worker error: {e}", exc_info=True)
            finally:
                try:
                    self._order_queue.task_done()
                except Exception:
                    pass

    def _enqueue_order(self, task: dict) -> bool:
        try:
            if "gen" not in task:
                task["gen"] = self._order_generation
            self._order_queue.put_nowait(task)
            return True
        except queue.Full:
            logger.error(
                f"Order queue full - dropping task {task.get('kind')} for {task.get('symbol')}"
            )
            return False

    def _mark_pending(self, stock: StockStatus, pending: str):
        stock.pending_order = pending
        stock.last_order_error = ""

    def _clear_pending(self, stock: StockStatus):
        stock.pending_order = ""

    @staticmethod
    def _match_position_symbol(pos_symbol: Any, symbol: str) -> bool:
        try:
            ps = str(pos_symbol or "").strip().upper()
            sym = str(symbol or "").strip().upper()
        except Exception:
            return False
        if not ps or not sym:
            return False
        if ps == sym:
            return True
        # Some brokers include series suffixes like "TCS-EQ"
        if ps.startswith(sym + "-"):
            return True
        if ps.endswith("-EQ") and ps[:-3] == sym:
            return True
        return False

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _get_broker_position_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Best-effort normalize broker position fields used to infer executed prices.
        Expected keys (when available):
          - buy_qty, sell_qty (int)
          - buy_avg, sell_avg (float)
        """
        try:
            if not getattr(self.dhan_client, "is_connected", False):
                return None
            positions = self.dhan_client.get_positions() or []
        except Exception:
            return None

        if not isinstance(positions, list):
            return None

        row = None
        for p in positions:
            if not isinstance(p, dict):
                continue
            ps = (
                p.get("tradingSymbol")
                or p.get("trading_symbol")
                or p.get("tradingsymbol")
                or p.get("symbol")
                or p.get("Symbol")
            )
            if self._match_position_symbol(ps, symbol):
                row = p
                break
        if row is None:
            return None

        def pick_int(*keys: str) -> int:
            for k in keys:
                if k in row and row.get(k) is not None:
                    v = self._coerce_int(row.get(k))
                    if v is not None:
                        return v
            return 0

        def pick_float(*keys: str) -> float:
            for k in keys:
                if k in row and row.get(k) is not None:
                    v = self._coerce_float(row.get(k))
                    if v is not None:
                        return v
            return 0.0

        snap = {
            "buy_qty": pick_int("buyQty", "buy_qty", "buyQuantity", "buy_quantity", "totalBuyQty", "total_buy_qty"),
            "sell_qty": pick_int("sellQty", "sell_qty", "sellQuantity", "sell_quantity", "totalSellQty", "total_sell_qty"),
            "buy_avg": pick_float("buyAvg", "buyAvgPrice", "buy_avg", "buy_average_price", "avgBuyPrice", "avg_buy_price"),
            "sell_avg": pick_float("sellAvg", "sellAvgPrice", "sell_avg", "sell_average_price", "avgSellPrice", "avg_sell_price"),
        }
        return snap

    @staticmethod
    def _infer_incremental_fill(before: Optional[dict], after: Optional[dict], transaction_type: str) -> Optional[tuple[float, int]]:
        """Infer incremental avg fill price/qty from before/after broker position snapshots."""
        if not after:
            return None
        tx = str(transaction_type or "").upper()
        if tx not in ("BUY", "SELL"):
            return None

        qty_key = "buy_qty" if tx == "BUY" else "sell_qty"
        avg_key = "buy_avg" if tx == "BUY" else "sell_avg"

        bq = int((before or {}).get(qty_key) or 0)
        ba = float((before or {}).get(avg_key) or 0.0)
        aq = int(after.get(qty_key) or 0)
        aa = float(after.get(avg_key) or 0.0)

        delta_qty = aq - bq
        if delta_qty <= 0:
            return None
        # Weighted-average delta: (avg*qty) is total value
        delta_value = (aa * aq) - (ba * bq)
        if delta_value <= 0:
            # If broker doesn't provide avg fields, fall back to unknown.
            return None
        return (delta_value / float(delta_qty), int(delta_qty))

    def _wait_for_broker_fill(self, symbol: str, transaction_type: str, before: Optional[dict], qty_hint: int) -> Optional[tuple[float, int]]:
        """
        Poll broker positions briefly and infer executed price/qty for the latest order.
        Falls back to None if positions are unavailable/not updated in time.
        """
        # Keep this short: runs on order worker threads.
        deadline = time.time() + 2.0
        best: Optional[tuple[float, int]] = None
        while time.time() < deadline:
            after = self._get_broker_position_snapshot(symbol)
            fill = self._infer_incremental_fill(before, after, transaction_type)
            if fill:
                best = fill
                # If we already saw at least requested qty (or more), stop early.
                if fill[1] >= max(1, int(qty_hint or 0)):
                    return fill
            time.sleep(0.2)
        return best

    def _place_market_order(self, symbol: str, transaction_type: str, qty: int, price: float):
        """Place a market order and record in OrderManager (runs in worker thread)."""
        if not self.is_market_hours():
            # Extra safety: never place orders before trade start time / after market close.
            logger.warning(
                f"Order blocked outside market hours (IST): {symbol} {transaction_type} qty={qty}"
            )
            return {"status": "failure", "message": "Order blocked: outside market hours (IST)"}, None, 0.0, 0

        with self._order_manager_lock:
            order = self.order_manager.create_order(
                symbol=symbol,
                transaction_type=transaction_type,
                quantity=qty,
                order_type="MARKET",
            )

        pos_before = self._get_broker_position_snapshot(symbol)
        start_time = time.time()
        resp = self.dhan_client.place_order(
            symbol=symbol,
            exchange_segment="NSE_EQ",
            transaction_type=transaction_type,
            quantity=qty,
            order_type="MARKET",
            product_type="INTRADAY",
        )
        perf_monitor.record_order_latency((time.time() - start_time) * 1000)

        if resp and resp.get("status") == "failure":
            try:
                # Keep OrderManager consistent for UI/debugging.
                with self._order_manager_lock:
                    if order:
                        self.order_manager.update_order_status(
                            order.order_id,
                            "REJECTED",
                            executed_price=0.0,
                            executed_quantity=0,
                            error_message=str(resp),
                        )
            except Exception:
                pass
            return resp, None, 0.0, 0

        order_id = None
        executed_price = float(price or 0.0)
        executed_qty = int(qty or 0)
        if resp:
            fallback_id = order.order_id if order else f"TEMP_{symbol}_{int(time.time() * 1000)}"
            order_id = str(resp.get("orderId", fallback_id))
            inferred = self._wait_for_broker_fill(symbol, transaction_type, pos_before, qty_hint=qty)
            if inferred:
                executed_price, executed_qty = float(inferred[0]), int(inferred[1])
            with self._order_manager_lock:
                if order:
                    self.order_manager.replace_order_id(order.order_id, order_id)
                    self.order_manager.update_order_status(
                        order_id,
                        "EXECUTED",
                        executed_price=executed_price,
                        executed_quantity=executed_qty,
                    )
        return resp, order_id, executed_price, executed_qty

    def _execute_order_task(self, task: dict):
        kind = task.get("kind")
        symbol = task.get("symbol")
        if not symbol or symbol not in self.active_stocks:
            return

        task_gen = task.get("gen")
        expected_pending = task.get("pending")
        if task_gen != self._order_generation:
            lock = self._get_stock_lock(symbol)
            with lock:
                stock = self.active_stocks.get(symbol)
                if stock and expected_pending and stock.pending_order == expected_pending:
                    stock.last_order_error = "Cancelled (engine stopped/restarted)"
                    if kind in ("START_LONG", "START_SHORT"):
                        stock.status = "IDLE"
                    elif kind in ("CLOSE", "CLOSE_AND_FLIP"):
                        stock.status = "ACTIVE"
                    self._clear_pending(stock)
            if kind in ("START_LONG", "START_SHORT"):
                with self._started_lock:
                    self._pending_start_symbols.discard(symbol)
            return

        lock = self._get_stock_lock(symbol)
        with lock:
            stock = self.active_stocks[symbol]
            if expected_pending and stock.pending_order != expected_pending:
                return

        if kind == "START_LONG":
            qty = int(task.get("qty") or 0)
            price = float(task.get("price") or 0.0)
            resp, order_id, exec_price, exec_qty = self._place_market_order(symbol, "BUY", qty, price)
            with lock:
                stock = self.active_stocks.get(symbol)
                if not stock or stock.pending_order != expected_pending:
                    return
                if resp and resp.get("status") == "failure":
                    stock.last_order_error = str(resp)
                    stock.status = "IDLE"
                    self._clear_pending(stock)
                    with self._started_lock:
                        self._pending_start_symbols.discard(symbol)
                    return

                with self._started_lock:
                    self._pending_start_symbols.discard(symbol)
                    self.started_symbols.add(symbol)

                if order_id:
                    stock.order_ids.append(str(order_id))

                fill_price = float(exec_price or price or 0.0)
                fill_qty = int(exec_qty or qty or 0)
                stock.mode = "LONG"
                stock.status = "ACTIVE"
                stock.ladder_level = 1
                stock.entry_price = fill_price
                stock.avg_entry_price = fill_price
                stock.quantity = fill_qty
                stock.high_watermark = fill_price

                stock.stop_loss = fill_price * (1 - self.init_sl_mult)
                stock.target = fill_price * (1 + self.target_mult)
                stock.next_add_on = fill_price * (1 + self.add_on_mult)
                self._clear_pending(stock)
            return

        if kind == "START_SHORT":
            qty = int(task.get("qty") or 0)
            price = float(task.get("price") or 0.0)
            resp, order_id, exec_price, exec_qty = self._place_market_order(symbol, "SELL", qty, price)
            with lock:
                stock = self.active_stocks.get(symbol)
                if not stock or stock.pending_order != expected_pending:
                    return
                if resp and resp.get("status") == "failure":
                    stock.last_order_error = str(resp)
                    stock.status = "IDLE"
                    self._clear_pending(stock)
                    with self._started_lock:
                        self._pending_start_symbols.discard(symbol)
                    return

                with self._started_lock:
                    self._pending_start_symbols.discard(symbol)
                    self.started_symbols.add(symbol)

                if order_id:
                    stock.order_ids.append(str(order_id))

                fill_price = float(exec_price or price or 0.0)
                fill_qty = int(exec_qty or qty or 0)
                stock.mode = "SHORT"
                stock.status = "ACTIVE"
                stock.ladder_level = 1
                stock.entry_price = fill_price
                stock.avg_entry_price = fill_price
                stock.quantity = fill_qty
                stock.high_watermark = fill_price

                stock.stop_loss = fill_price * (1 + self.init_sl_mult)
                stock.target = fill_price * (1 - self.target_mult)
                stock.next_add_on = fill_price * (1 - self.add_on_mult)
                self._clear_pending(stock)
            return

        if kind == "ADD_ON":
            mode = task.get("mode")
            qty = int(task.get("qty") or 0)
            price = float(task.get("price") or 0.0)
            transaction_type = "BUY" if mode == "LONG" else "SELL"
            resp, order_id, exec_price, exec_qty = self._place_market_order(symbol, transaction_type, qty, price)
            with lock:
                stock = self.active_stocks.get(symbol)
                if not stock or stock.pending_order != expected_pending:
                    return
                if resp and resp.get("status") == "failure":
                    stock.last_order_error = str(resp)
                    self._clear_pending(stock)
                    return

                if order_id:
                    stock.order_ids.append(str(order_id))

                fill_price = float(exec_price or price or 0.0)
                fill_qty = int(exec_qty or qty or 0)
                prev_qty = stock.quantity
                stock.quantity += fill_qty
                stock.ladder_level += 1 if fill_qty > 0 else 0

                if prev_qty > 0 and stock.avg_entry_price > 0:
                    stock.avg_entry_price = (
                        (stock.avg_entry_price * prev_qty) + (fill_price * fill_qty)
                    ) / stock.quantity
                else:
                    stock.avg_entry_price = fill_price

                if mode == "LONG":
                    stock.next_add_on = fill_price * (1 + self.add_on_mult)
                    # Keep SL/target aligned to executed (avg) price; never loosen SL.
                    init_sl = stock.avg_entry_price * (1 - self.init_sl_mult)
                    if init_sl > stock.stop_loss:
                        stock.stop_loss = init_sl
                    stock.target = stock.avg_entry_price * (1 + self.target_mult)
                else:
                    stock.next_add_on = fill_price * (1 - self.add_on_mult)
                    init_sl = stock.avg_entry_price * (1 + self.init_sl_mult)
                    if stock.stop_loss == 0 or init_sl < stock.stop_loss:
                        stock.stop_loss = init_sl
                    stock.target = stock.avg_entry_price * (1 - self.target_mult)

                self._clear_pending(stock)
            return

        if kind == "CLOSE":
            transaction_type = task.get("transaction_type")
            qty = int(task.get("qty") or 0)
            price = float(task.get("price") or 0.0)
            final_status = task.get("final_status") or "CLOSED"
            resp, order_id, _, _ = self._place_market_order(symbol, transaction_type, qty, price)
            with lock:
                stock = self.active_stocks.get(symbol)
                if not stock or stock.pending_order != expected_pending:
                    return
                if resp and resp.get("status") == "failure":
                    stock.last_order_error = str(resp)
                    stock.status = "ACTIVE"
                    self._clear_pending(stock)
                    return

                if order_id:
                    stock.order_ids.append(str(order_id))

                stock.quantity = 0
                stock.mode = "NONE"
                stock.status = final_status
                self._clear_pending(stock)
            return

        if kind == "CLOSE_AND_FLIP":
            reverse_tx = task.get("reverse_transaction_type") or task.get("close_transaction_type")
            close_qty = int(task.get("close_qty") or 0)
            reverse_qty = int(task.get("reverse_qty") or 0)
            flip_to = task.get("flip_to")
            open_qty = int(task.get("open_qty") or 0)
            price = float(task.get("price") or task.get("close_price") or task.get("open_price") or 0.0)
            cycle_index_next = task.get("cycle_index_next")

            if reverse_qty <= 0:
                reverse_qty = close_qty + max(0, open_qty)

            resp_rev, order_id_rev, exec_price, exec_qty = self._place_market_order(
                symbol, reverse_tx, reverse_qty, price
            )
            with lock:
                stock = self.active_stocks.get(symbol)
                if not stock or stock.pending_order != expected_pending:
                    return

                if resp_rev and resp_rev.get("status") == "failure":
                    stock.last_order_error = str(resp_rev)
                    stock.status = "ACTIVE"
                    self._clear_pending(stock)
                    return

                if order_id_rev:
                    stock.order_ids.append(str(order_id_rev))

                # Start new ladder in opposite direction (level resets)
                with self._started_lock:
                    self.started_symbols.add(symbol)

                fill_price = float(exec_price or price or 0.0)
                filled_total = int(exec_qty or reverse_qty or 0)
                filled_open_qty = max(0, filled_total - max(0, close_qty))

                # If we couldn't open the next ladder, keep best-effort stable state.
                if filled_open_qty <= 0:
                    stock.last_order_error = "Flip executed without opening new ladder quantity"
                    stock.quantity = 0
                    stock.mode = "NONE"
                    stock.status = "IDLE"
                    self._clear_pending(stock)
                    return

                if flip_to == "SHORT":
                    stock.mode = "SHORT"
                    stock.stop_loss = fill_price * (1 + self.init_sl_mult)
                    stock.target = fill_price * (1 - self.target_mult)
                    stock.next_add_on = fill_price * (1 - self.add_on_mult)
                else:
                    stock.mode = "LONG"
                    stock.stop_loss = fill_price * (1 - self.init_sl_mult)
                    stock.target = fill_price * (1 + self.target_mult)
                    stock.next_add_on = fill_price * (1 + self.add_on_mult)

                stock.status = "ACTIVE"
                stock.ladder_level = 1
                stock.entry_price = fill_price
                stock.avg_entry_price = fill_price
                stock.quantity = filled_open_qty
                stock.high_watermark = fill_price
                if cycle_index_next is not None:
                    try:
                        stock.cycle_index = int(cycle_index_next)
                    except Exception:
                        pass
                self._clear_pending(stock)
            return

    def _update_multipliers(self):
        """Pre-calculate percentage multipliers for performance."""
        self.add_on_mult = self.settings.add_on_percentage / 100
        self.init_sl_mult = self.settings.initial_stop_loss_pct / 100
        self.tsl_mult = self.settings.trailing_stop_loss_pct / 100
        self.target_mult = self.settings.target_percentage / 100

    def update_settings(self, new_settings: StrategySettings):
        new_settings = self._normalize_settings(new_settings)
        self.settings = new_settings
        self._ensure_order_workers()
        self._update_multipliers()
        # Avoid logging sensitive tokens
        safe_settings = self.settings.model_dump()
        if safe_settings.get("access_token"):
            safe_settings["access_token"] = "***"
        logger.info(f"Settings Updated: {safe_settings}")

    def stop(self, reason: str = "Stopped"):
        """Stop strategy and cancel queued (not-yet-executed) orders."""
        self.running = False
        self.armed_for_market_open = False
        self._order_generation += 1
        with self._started_lock:
            self._pending_start_symbols.clear()

        # Best-effort clear any queued tasks (workers may still be executing one).
        try:
            while True:
                _ = self._order_queue.get_nowait()
                try:
                    self._order_queue.task_done()
                except Exception:
                    pass
        except queue.Empty:
            pass

        # Mark any pending orders as cancelled so UI doesn't get stuck.
        for s in self.active_stocks.values():
            if getattr(s, "pending_order", ""):
                s.last_order_error = f"Cancelled: {reason}"
                s.pending_order = ""
                if s.status.startswith("PENDING"):
                    # Revert to best-effort stable state
                    s.status = "ACTIVE" if s.mode != "NONE" else "IDLE"

    def _normalize_settings(self, settings: StrategySettings) -> StrategySettings:
        """Clamp/adjust settings to avoid invalid combinations from UI."""
        try:
            max_ladders = int(settings.max_ladder_stocks or 0)
        except Exception:
            max_ladders = 0

        try:
            top_gainers = int(settings.top_n_gainers or 0)
        except Exception:
            top_gainers = 0

        try:
            top_losers = int(settings.top_n_losers or 0)
        except Exception:
            top_losers = 0

        try:
            max_concurrent_orders = int(getattr(settings, "max_concurrent_orders", 2) or 2)
        except Exception:
            max_concurrent_orders = 2

        max_ladders = max(1, max_ladders)
        top_gainers = max(0, top_gainers)
        top_losers = max(0, top_losers)
        max_concurrent_orders = max(1, max_concurrent_orders)

        try:
            cycles_per_stock = int(getattr(settings, "cycles_per_stock", 3) or 3)
        except Exception:
            cycles_per_stock = 3
        cycles_per_stock = max(1, cycles_per_stock)

        # Rule: top_gainers + top_losers must not exceed max_ladders (when max_ladders > 0)
        if max_ladders > 0 and (top_gainers + top_losers) > max_ladders:
            # Prefer keeping gainers as-is and reduce losers first.
            top_losers = max(0, max_ladders - top_gainers)
            if top_losers == 0 and top_gainers > max_ladders:
                top_gainers = max_ladders

        return settings.model_copy(
            update={
                "max_ladder_stocks": max_ladders,
                "top_n_gainers": top_gainers,
                "top_n_losers": top_losers,
                "max_concurrent_orders": max_concurrent_orders,
                "cycles_per_stock": cycles_per_stock,
            }
        )

    def is_market_hours(self) -> bool:
        """Check if current time is within market hours."""
        now = datetime.now(IST).time()
        market_open = dt_time(9, 16)
        market_close = dt_time(15, 30)  # Market closes at 3:30 PM
        return market_open <= now <= market_close

    @staticmethod
    def _env_truthy(name: str) -> bool:
        val = os.getenv(name, "")
        return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _write_movers_diagnostics(self, payload: dict) -> None:
        path = os.getenv("MOVERS_DIAGNOSTICS_PATH", "movers_diagnostics.json").strip() or "movers_diagnostics.json"
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Mover diagnostics saved to {path}")
        except Exception as e:
            logger.warning(f"Failed to write mover diagnostics to {path}: {e}")

    def _build_mover_diagnostics_payload(self, stocks: List[StockStatus], *, source: str) -> dict:
        min_turnover = float(self.settings.min_turnover_crores) * 10000000
        now_ts = datetime.now(IST).isoformat()

        def _rec(s: StockStatus) -> dict:
            reasons: list[str] = []
            status = getattr(s, "status", "")
            ltp = float(getattr(s, "ltp", 0.0) or 0.0)
            volume = float(getattr(s, "last_volume", 0.0) or 0.0)
            turnover = float(getattr(s, "turnover", 0.0) or 0.0)
            prev_close = float(getattr(s, "prev_close", 0.0) or 0.0)
            change_pct = float(getattr(s, "change_pct", 0.0) or 0.0)

            if status != "IDLE":
                reasons.append("NOT_IDLE")
            if ltp <= 0:
                reasons.append("LTP_LEQ_0")
            if turnover < min_turnover:
                reasons.append("TURNOVER_BELOW_MIN")
            if prev_close <= 0:
                reasons.append("PREV_CLOSE_LEQ_0")
            if change_pct == 0.0:
                reasons.append("NO_MOVE")

            return {
                "symbol": getattr(s, "symbol", ""),
                "status": status,
                "ltp": ltp,
                "volume": volume,
                "turnover": turnover,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "reasons": reasons,
            }

        records = [_rec(s) for s in stocks]

        eligible = [
            r
            for r in records
            if (
                r["status"] == "IDLE"
                and r["ltp"] > 0
                and r["turnover"] >= min_turnover
                and r["prev_close"] > 0
                and r["change_pct"] != 0.0
            )
        ]

        ineligible = [r for r in records if r not in eligible]

        reason_counts = Counter()
        for r in ineligible:
            for reason in r.get("reasons") or []:
                reason_counts[reason] += 1

        return {
            "timestamp_ist": now_ts,
            "source": source,
            "min_turnover": min_turnover,
            "min_turnover_crores": float(self.settings.min_turnover_crores),
            "total_tracked": len(records),
            "eligible_for_ranking": len(eligible),
            "ineligible": len(ineligible),
            "ineligible_reason_counts": dict(reason_counts),
            "stocks": records,
        }

    def _maybe_emit_movers_diagnostics(self, *, source: str) -> None:
        if not self._env_truthy("MOVERS_DIAGNOSTICS"):
            return

        try:
            interval_s = float(os.getenv("MOVERS_DIAGNOSTICS_INTERVAL_SECONDS", "15") or "15")
        except Exception:
            interval_s = 15.0
        interval_s = max(1.0, interval_s)

        now = time.time()
        if (now - float(self._last_movers_diag_ts)) < interval_s:
            return
        self._last_movers_diag_ts = now

        payload = self._build_mover_diagnostics_payload(list(self.active_stocks.values()), source=source)
        self._write_movers_diagnostics(payload)

        counts = payload.get("ineligible_reason_counts") or {}
        logger.info(
            "Mover diagnostics summary: "
            f"tracked={payload.get('total_tracked')}, eligible={payload.get('eligible_for_ranking')}, "
            f"ineligible={payload.get('ineligible')}, reasons={counts}"
        )

        if self._env_truthy("MOVERS_DIAGNOSTICS_LOG_ALL"):
            for r in payload.get("stocks") or []:
                if r.get("reasons"):
                    logger.info(
                        "MOVER_FILTER_OUT "
                        f"{r.get('symbol')} status={r.get('status')} "
                        f"ltp={r.get('ltp'):.2f} volume={r.get('volume'):.0f} "
                        f"turnover={r.get('turnover'):.0f} prev_close={r.get('prev_close'):.2f} "
                        f"change_pct={r.get('change_pct'):.2f} reasons={','.join(r.get('reasons'))}"
                    )

    def _diagnose_movers_closed_market(self) -> None:
        """
        When the market is closed, the websocket engine doesn't start. This diagnostic mode
        fetches an OHLC snapshot via REST (best-effort) and outputs a mover-filter report.
        Enabled via MOVERS_DIAGNOSTICS=1.
        """
        try:
            candidates_map = self.load_filtered_stocks()
            if not candidates_map:
                logger.warning("Mover diagnostics: no candidates available (filtered_stocks.json empty/missing)")
                return

            symbols = list(candidates_map.keys())
            snap = self.dhan_client.get_ohlc_snapshot(symbols)

            tmp: Dict[str, StockStatus] = {}
            for sym in symbols:
                d = snap.get(sym) or {}
                ltp = float(d.get("ltp") or 0.0)
                prev_close = float(d.get("prev_close") or candidates_map.get(sym) or 0.0)
                volume = float(d.get("volume") or 0.0)
                turnover = float(d.get("turnover") or 0.0)
                change_pct = float(d.get("change_pct") or 0.0)

                tmp[sym] = StockStatus(
                    symbol=sym,
                    mode="NONE",
                    ltp=ltp,
                    change_pct=change_pct,
                    pnl=0.0,
                    status="IDLE",
                    entry_price=0.0,
                    quantity=0,
                    ladder_level=0,
                    next_add_on=0.0,
                    stop_loss=0.0,
                    target=0.0,
                    prev_close=prev_close,
                    last_volume=volume,
                    turnover=turnover,
                    high_watermark=0.0,
                )

            payload = self._build_mover_diagnostics_payload(list(tmp.values()), source="closed_market_rest")
            for r in payload.get("stocks") or []:
                if r.get("symbol") not in snap:
                    r.setdefault("reasons", []).append("NO_REST_SNAPSHOT")

            self._write_movers_diagnostics(payload)
            logger.info(
                "Mover diagnostics (market closed) complete. "
                "REST values can be stale outside market hours."
            )
        except Exception as e:
            logger.warning(f"Mover diagnostics (market closed) failed: {e}")

    async def start_strategy(self):
        if not self.dhan_client.is_connected:
            logger.error("Cannot start: Dhan client not connected")
            return

        # If market is closed, don't start WebSocket feed; dashboard will use REST top-movers.
        if not self.is_market_hours():
            logger.info("Market closed - not starting WebSocket feed (use Top Movers API fallback)")
            if self._env_truthy("MOVERS_DIAGNOSTICS"):
                self._diagnose_movers_closed_market()
            self.running = False
            return

        self.running = True
        self.trading_halted = False
        self.trading_halt_reason = ""
        self._order_generation += 1
        # Reset per-run state (so max ladder stocks applies per session)
        self.started_symbols.clear()
        with self._started_lock:
            self._pending_start_symbols.clear()
        self._last_select_ts = 0.0
        logger.info("Strategy Engine Started")
        
        # Load pre-filtered stocks from JSON
        candidates_map = self.load_filtered_stocks()
        if not candidates_map:
            logger.error("No filtered stocks available. Run premarket_filter.py first!")
            self.running = False
            return
            
        candidates = list(candidates_map.keys())
        logger.info(f"Loaded {len(candidates)} pre-filtered candidates from filtered_stocks.json")
        
        # Subscribe to WebSocket
        self.dhan_client.subscribe(candidates, self.process_tick)
        
        # Initialize stocks in tracking dict
        for symbol in candidates:
            self.active_stocks[symbol] = StockStatus(
                symbol=symbol,
                mode="NONE",
                ltp=0.0,
                change_pct=0.0,
                pnl=0.0,
                status="IDLE",
                entry_price=0.0,
                quantity=0,
                ladder_level=0,
                next_add_on=0.0,
                stop_loss=0.0,
                target=0.0,
                prev_close=candidates_map[symbol],
                high_watermark=0.0
            )

        # Main loop
        while self.running:
            await asyncio.sleep(1)
            self.calculate_pnl()

            # Global P&L exits (if configured)
            try:
                profit_exit = float(getattr(self.settings, "global_profit_exit", 0.0) or 0.0)
            except Exception:
                profit_exit = 0.0
            try:
                loss_exit_raw = float(getattr(self.settings, "global_loss_exit", 0.0) or 0.0)
            except Exception:
                loss_exit_raw = 0.0
            loss_exit = abs(loss_exit_raw)

            if not self.trading_halted:
                if profit_exit > 0 and self.pnl_global >= profit_exit:
                    self.trading_halted = True
                    self.trading_halt_reason = f"Global P&L target reached ({self.pnl_global:.2f} >= {profit_exit:.2f})"
                    logger.warning(self.trading_halt_reason)
                    await self.square_off_all(
                        reason="Global P&L target reached",
                        final_status="CLOSED_GLOBAL_PROFIT",
                    )
                elif loss_exit > 0 and self.pnl_global <= -loss_exit:
                    self.trading_halted = True
                    self.trading_halt_reason = f"Global P&L loss limit reached ({self.pnl_global:.2f} <= {-loss_exit:.2f})"
                    logger.warning(self.trading_halt_reason)
                    await self.square_off_all(
                        reason="Global P&L loss limit reached",
                        final_status="CLOSED_GLOBAL_LOSS",
                    )
            
            # Auto square-off at 3:20 PM
            if not self.is_market_hours() and self.running:
                logger.info("Market closed - Auto square-off triggered")
                await self.square_off_all()
                # Stop feed to avoid reconnect storms after market close
                self.dhan_client.stop_feed()
                self.running = False

    def process_tick(self, symbol: str, ltp: float, volume: float = 0.0):
        """Process incoming tick data with performance tracking."""
        start_time = time.time()
        
        if not self.running or symbol not in self.active_stocks:
            return

        lock = self._get_stock_lock(symbol)
        # Never block tick thread: skip tick if this stock is being updated by an order worker.
        if not lock.acquire(blocking=False):
            return
        try:
            stock = self.active_stocks[symbol]

            if stock.status == "STOPPED" or stock.status.startswith("CLOSED"):
                return

            # Update LTP and turnover
            stock.ltp = ltp
            stock.last_volume = float(volume or 0.0)
            if volume > 0:
                stock.turnover = volume * ltp

            # Capture "day open" approximation from first observed tick.
            if getattr(stock, "day_open", 0.0) <= 0 and ltp > 0 and stock.prev_close > 0:
                stock.day_open = float(ltp)
                stock.open_gap_pct = ((stock.day_open - stock.prev_close) / stock.prev_close) * 100.0

            # Calculate % Change
            if stock.prev_close > 0:
                stock.change_pct = ((ltp - stock.prev_close) / stock.prev_close) * 100

            # Update high watermark for trailing SL
            if stock.mode != "NONE":
                if stock.mode == "LONG" and ltp > stock.high_watermark:
                    stock.high_watermark = ltp
                elif stock.mode == "SHORT" and (
                    stock.high_watermark == 0 or ltp < stock.high_watermark
                ):
                    stock.high_watermark = ltp

            # Calculate P&L using cached avg entry price (updated on executions)
            if stock.mode != "NONE" and stock.quantity > 0 and stock.avg_entry_price > 0:
                if stock.mode == "LONG":
                    stock.pnl = (ltp - stock.avg_entry_price) * stock.quantity
                else:
                    stock.pnl = (stock.avg_entry_price - ltp) * stock.quantity

            # If trading is halted, don't take any new actions (but keep updating UI fields).
            if self.trading_halted:
                latency_ms = (time.time() - start_time) * 1000
                perf_monitor.record_tick_latency(latency_ms)
                return

            # If an order is in-flight for this stock, don't trigger new actions.
            if getattr(stock, "pending_order", ""):
                latency_ms = (time.time() - start_time) * 1000
                perf_monitor.record_tick_latency(latency_ms)
                return

            # Trading Logic
            if stock.mode == "LONG":
                self._process_long_position(stock)
            elif stock.mode == "SHORT":
                self._process_short_position(stock)

            # Per-stock P&L limits
            try:
                profit_target = float(getattr(self.settings, "profit_target_per_stock", 0.0) or 0.0)
            except Exception:
                profit_target = 0.0
            try:
                loss_limit_raw = float(getattr(self.settings, "loss_limit_per_stock", 0.0) or 0.0)
            except Exception:
                loss_limit_raw = 0.0
            loss_limit = abs(loss_limit_raw)

            if profit_target > 0 and stock.pnl >= profit_target:
                self.close_position(stock, "Stock profit target reached", final_status="CLOSED_STOCK_PROFIT_LIMIT")
            elif loss_limit > 0 and stock.pnl <= -loss_limit:
                self.close_position(stock, "Stock loss limit reached", final_status="CLOSED_STOCK_LOSS_LIMIT")

            # Reactive mover selection (throttled)
            self._maybe_select_top_movers()

            # Record performance
            latency_ms = (time.time() - start_time) * 1000
            perf_monitor.record_tick_latency(latency_ms)
        finally:
            try:
                lock.release()
            except Exception:
                pass

    def _process_long_position(self, stock: StockStatus):
        """Process LONG position logic."""
        # 1. Check Target
        if stock.ltp >= stock.target:
            self._finish_ladder_cycle(stock, reason="Target Hit")
            return

        # 2. Check Stop Loss / TSL
        if stock.ltp <= stock.stop_loss:
            self._finish_ladder_cycle(stock, reason="SL Hit")
            return

        # 3. Add-on Logic (Pyramiding)
        if stock.ladder_level < self.settings.no_of_add_ons:
            if stock.ltp >= stock.next_add_on:
                self.execute_add_on(stock, "LONG")
        
        # 4. Update Trailing SL using high watermark
        if stock.high_watermark > 0:
            dynamic_sl = stock.high_watermark * (1 - self.tsl_mult)
            if dynamic_sl > stock.stop_loss:
                stock.stop_loss = dynamic_sl

    def _process_short_position(self, stock: StockStatus):
        """Process SHORT position logic."""
        # 1. Check Target
        if stock.ltp <= stock.target:
            self._finish_ladder_cycle(stock, reason="Target Hit")
            return

        # 2. Check SL
        if stock.ltp >= stock.stop_loss:
            self._finish_ladder_cycle(stock, reason="SL Hit")
            return

        # 3. Add-on Logic
        if stock.ladder_level < self.settings.no_of_add_ons:
            if stock.ltp <= stock.next_add_on:
                self.execute_add_on(stock, "SHORT")
        
        # 4. TSL
        if stock.high_watermark > 0:
            dynamic_sl = stock.high_watermark * (1 + self.tsl_mult)
            if dynamic_sl < stock.stop_loss or stock.stop_loss == 0:
                stock.stop_loss = dynamic_sl

    def _close_and_flip(self, stock: StockStatus, flip_to: str, reason: str, *, cycle_index_next: int | None = None):
        """Close current position and open opposite direction without blocking tick thread."""
        symbol = stock.symbol
        lock = self._get_stock_lock(symbol)
        with lock:
            if stock.pending_order:
                return
            if stock.quantity <= 0 or stock.mode == "NONE":
                return

            if flip_to not in ("LONG", "SHORT"):
                return

            close_tx = "SELL" if stock.mode == "LONG" else "BUY"
            close_qty = int(stock.quantity)

            # Initial quantity for the next ladder (based on current LTP).
            open_qty = max(1, int(self.settings.trade_capital / stock.ltp)) if stock.ltp > 0 else 1
            # One reverse order: close existing qty + open initial qty for next ladder.
            reverse_tx = close_tx
            reverse_qty = int(close_qty + open_qty)
            price = float(stock.ltp)

            pending = f"CLOSE_AND_FLIP_{flip_to}"
            self._mark_pending(stock, pending)
            stock.status = "PENDING_FLIP"

            task = {
                "kind": "CLOSE_AND_FLIP",
                "symbol": symbol,
                "pending": pending,
                "reason": reason,
                "close_transaction_type": close_tx,
                "close_qty": close_qty,
                "flip_to": flip_to,
                "open_qty": open_qty,
                "reverse_transaction_type": reverse_tx,
                "reverse_qty": reverse_qty,
                "price": price,
                "cycle_index_next": cycle_index_next,
            }

        if not self._enqueue_order(task):
            with lock:
                stock.last_order_error = "Order queue full"
                stock.status = "ACTIVE"
                self._clear_pending(stock)

    def _finish_ladder_cycle(self, stock: StockStatus, *, reason: str):
        """
        Enforce 3 alternating cycles per stock (configured via settings.cycles_per_stock):
          - Gainer (starts LONG): LONG -> SHORT -> LONG -> CLOSE
          - Loser (starts SHORT): SHORT -> LONG -> SHORT -> CLOSE
        """
        try:
            cycle_total = int(getattr(stock, "cycle_total", 1) or 1)
        except Exception:
            cycle_total = 1
        try:
            cycle_index = int(getattr(stock, "cycle_index", 0) or 0)
        except Exception:
            cycle_index = 0

        if cycle_total <= 1:
            self.close_position(stock, reason, final_status="CLOSED")
            return

        # Last cycle: close and stop further ladders for this stock.
        if (cycle_index + 1) >= cycle_total:
            self.close_position(stock, f"{reason} (cycles completed)", final_status="CLOSED_CYCLES")
            return

        flip_to = "SHORT" if stock.mode == "LONG" else "LONG"
        next_index = cycle_index + 1
        self._close_and_flip(
            stock,
            flip_to=flip_to,
            reason=f"{reason} (cycle {next_index+1}/{cycle_total})",
            cycle_index_next=next_index,
        )

    def execute_add_on(self, stock: StockStatus, mode: str):
        """Execute add-on order with tracking."""
        symbol = stock.symbol
        lock = self._get_stock_lock(symbol)
        with lock:
            if stock.pending_order:
                return

            qty = (
                max(1, int(self.settings.trade_capital / stock.entry_price))
                if stock.entry_price > 0
                else 1
            )
            pending = f"ADD_ON_{mode}"
            self._mark_pending(stock, pending)

            task = {
                "kind": "ADD_ON",
                "symbol": symbol,
                "pending": pending,
                "mode": mode,
                "qty": qty,
                "price": float(stock.ltp),
            }

        logger.info(f"Queued ADD-ON for {symbol} ({mode}) Qty: {qty}")
        if not self._enqueue_order(task):
            with lock:
                stock.last_order_error = "Order queue full"
                self._clear_pending(stock)

    def close_position(self, stock: StockStatus, reason: str, final_status: str = "CLOSED"):
        """Queue a close order (non-blocking)."""
        symbol = stock.symbol
        lock = self._get_stock_lock(symbol)
        with lock:
            if stock.pending_order:
                return
            if stock.quantity <= 0 or stock.mode == "NONE":
                return

            transaction_type = "SELL" if stock.mode == "LONG" else "BUY"
            qty = int(stock.quantity)
            price = float(stock.ltp)
            pending = "CLOSE"
            self._mark_pending(stock, pending)
            stock.status = "PENDING_CLOSE"

            task = {
                "kind": "CLOSE",
                "symbol": symbol,
                "pending": pending,
                "reason": reason,
                "transaction_type": transaction_type,
                "qty": qty,
                "price": price,
                "final_status": final_status,
            }

        logger.info(f"Queued CLOSE for {symbol}: {reason}")
        if not self._enqueue_order(task):
            with lock:
                stock.last_order_error = "Order queue full"
                stock.status = "ACTIVE"
                self._clear_pending(stock)

    def start_long_ladder(self, stock: StockStatus):
        """Queue LONG ladder start (non-blocking)."""
        symbol = stock.symbol
        with self._started_lock:
            started_or_pending = len(self.started_symbols) + len(self._pending_start_symbols)
        if symbol not in self.started_symbols and symbol not in self._pending_start_symbols and started_or_pending >= self.settings.max_ladder_stocks:
            logger.info(
                f"SKIP LONG {symbol}: max ladder stocks reached "
                f"({started_or_pending}/{self.settings.max_ladder_stocks})"
            )
            return

        lock = self._get_stock_lock(symbol)
        with lock:
            if stock.pending_order or stock.status != "IDLE":
                return
            if int(getattr(stock, "cycle_total", 1) or 1) <= 1:
                try:
                    cycles_total = int(getattr(self.settings, "cycles_per_stock", 3) or 3)
                except Exception:
                    cycles_total = 3
                stock.cycle_total = max(1, cycles_total)
                stock.cycle_index = 0
                stock.cycle_start_mode = "LONG"
            qty = max(1, int(self.settings.trade_capital / stock.ltp)) if stock.ltp > 0 else 1
            pending = "START_LONG"
            self._mark_pending(stock, pending)
            stock.status = "PENDING_LONG"
            with self._started_lock:
                self._pending_start_symbols.add(symbol)

            task = {
                "kind": "START_LONG",
                "symbol": symbol,
                "pending": pending,
                "qty": qty,
                "price": float(stock.ltp),
            }

        logger.info(f"Queued LONG start for {symbol}")
        if not self._enqueue_order(task):
            with lock:
                stock.last_order_error = "Order queue full"
                stock.status = "IDLE"
                self._clear_pending(stock)
            with self._started_lock:
                self._pending_start_symbols.discard(symbol)

    def start_short_ladder(self, stock: StockStatus):
        """Queue SHORT ladder start (non-blocking)."""
        symbol = stock.symbol
        with self._started_lock:
            started_or_pending = len(self.started_symbols) + len(self._pending_start_symbols)
        if symbol not in self.started_symbols and symbol not in self._pending_start_symbols and started_or_pending >= self.settings.max_ladder_stocks:
            logger.info(
                f"SKIP SHORT {symbol}: max ladder stocks reached "
                f"({started_or_pending}/{self.settings.max_ladder_stocks})"
            )
            return

        lock = self._get_stock_lock(symbol)
        with lock:
            if stock.pending_order or stock.status != "IDLE":
                return
            if int(getattr(stock, "cycle_total", 1) or 1) <= 1:
                try:
                    cycles_total = int(getattr(self.settings, "cycles_per_stock", 3) or 3)
                except Exception:
                    cycles_total = 3
                stock.cycle_total = max(1, cycles_total)
                stock.cycle_index = 0
                stock.cycle_start_mode = "SHORT"
            qty = max(1, int(self.settings.trade_capital / stock.ltp)) if stock.ltp > 0 else 1
            pending = "START_SHORT"
            self._mark_pending(stock, pending)
            stock.status = "PENDING_SHORT"
            with self._started_lock:
                self._pending_start_symbols.add(symbol)

            task = {
                "kind": "START_SHORT",
                "symbol": symbol,
                "pending": pending,
                "qty": qty,
                "price": float(stock.ltp),
            }

        logger.info(f"Queued SHORT start for {symbol}")
        if not self._enqueue_order(task):
            with lock:
                stock.last_order_error = "Order queue full"
                stock.status = "IDLE"
                self._clear_pending(stock)
            with self._started_lock:
                self._pending_start_symbols.discard(symbol)

    def calculate_pnl(self):
        """Calculate total P&L using NumPy for performance."""
        pnl_values = [s.pnl for s in self.active_stocks.values()]
        self.pnl_global = np.sum(pnl_values) if pnl_values else 0.0

    def _maybe_select_top_movers(self):
        """Run mover selection at most once per interval (reactive)."""
        import time
        now = time.time()
        if (now - self._last_select_ts) < self._select_interval_seconds:
            return
        self._last_select_ts = now
        self.select_top_movers()

    def select_top_movers(self):
        """Rank stocks and activate ladders for top movers."""
        min_turnover = self.settings.min_turnover_crores * 10000000

        logger.info(
            f"Selecting movers: min_turnover={self.settings.min_turnover_crores:.2f} Cr "
            f"({min_turnover:.0f})"
        )
        
        active_longs = 0
        active_shorts = 0
        pending_longs = 0
        pending_shorts = 0
        for s in self.active_stocks.values():
            if s.pending_order == "START_LONG":
                pending_longs += 1
            elif s.pending_order == "START_SHORT":
                pending_shorts += 1

            if s.quantity <= 0:
                continue
            if s.mode == "LONG":
                active_longs += 1
            elif s.mode == "SHORT":
                active_shorts += 1

        active_total = active_longs + active_shorts + pending_longs + pending_shorts
        max_ladders = max(1, int(self.settings.max_ladder_stocks or 0))

        with self._started_lock:
            started_or_pending = len(self.started_symbols) + len(self._pending_start_symbols)

        if started_or_pending >= max_ladders:
            logger.info(
                f"Max ladder stocks reached for session ({started_or_pending}/{max_ladders}) "
                f"- not starting new symbols"
            )
            return

        if active_total >= max_ladders:
            logger.info(
                f"Max ladder stocks reached ({active_total}/{max_ladders}) - not starting new ladders"
            )
            return

        # Only start as many as needed to reach configured targets.
        need_longs = max(0, int(self.settings.top_n_gainers or 0) - (active_longs + pending_longs))
        need_shorts = max(0, int(self.settings.top_n_losers or 0) - (active_shorts + pending_shorts))

        remaining_capacity = max(0, max_ladders - active_total)
        if (need_longs + need_shorts) > remaining_capacity:
            # Keep longs priority, then shorts, under remaining capacity.
            need_longs = min(need_longs, remaining_capacity)
            need_shorts = min(need_shorts, max(0, remaining_capacity - need_longs))

        if need_longs <= 0 and need_shorts <= 0:
            logger.info(
                f"No new ladders needed (active_longs={active_longs}/{self.settings.top_n_gainers}, "
                f"active_shorts={active_shorts}/{self.settings.top_n_losers}, "
                f"max={max_ladders})"
            )
            return

        idle_stocks = []
        for s in self.active_stocks.values():
            if s.status != "IDLE":
                continue
            if s.ltp <= 0:
                logger.debug(f"FILTERED {s.symbol}: LTP<=0 (ltp={s.ltp})")
                continue
            if s.turnover < min_turnover:
                logger.debug(
                    f"FILTERED {s.symbol}: Turnover below threshold "
                    f"(turnover={s.turnover:.0f}, min={min_turnover:.0f})"
                )
                continue
            idle_stocks.append(s)

        # Optional diagnostics: dump why symbols are being filtered out (without changing workflows).
        # Enable with MOVERS_DIAGNOSTICS=1 (and MOVERS_DIAGNOSTICS_LOG_ALL=1 for per-symbol logs).
        self._maybe_emit_movers_diagnostics(source="ws_select_top_movers")
        
        if not idle_stocks:
            logger.info("No eligible idle stocks after turnover/LTP filters")
            return
            
        try:
            max_gap_long = float(getattr(self.settings, "max_open_gap_pct_long", 3.0) or 3.0)
        except Exception:
            max_gap_long = 3.0
        try:
            min_gap_short = float(getattr(self.settings, "min_open_gap_pct_short", -3.0) or -3.0)
        except Exception:
            min_gap_short = -3.0

        def _gap_pct(s: StockStatus) -> float:
            if getattr(s, "day_open", 0.0) > 0 and getattr(s, "prev_close", 0.0) > 0:
                try:
                    return float(getattr(s, "open_gap_pct", 0.0) or 0.0)
                except Exception:
                    return 0.0
            if getattr(s, "prev_close", 0.0) > 0:
                open_px = float(getattr(s, "ltp", 0.0) or 0.0)
                if open_px > 0:
                    return ((open_px - float(s.prev_close)) / float(s.prev_close)) * 100.0
            return 0.0

        long_candidates = [s for s in idle_stocks if s.change_pct > 0 and _gap_pct(s) <= max_gap_long]
        short_candidates = [s for s in idle_stocks if s.change_pct < 0 and _gap_pct(s) >= min_gap_short]

        # Sort by % Change
        long_candidates.sort(key=lambda x: x.change_pct, reverse=True)
        short_candidates.sort(key=lambda x: x.change_pct)  # Most negative first

        top_gainers = long_candidates[:need_longs] if need_longs > 0 else []
        top_losers = short_candidates[:need_shorts] if need_shorts > 0 else []

        if top_gainers:
            logger.info(
                "Top Gainers (selected): " + ", ".join(
                    f"{s.symbol}({s.change_pct:.2f}%, turnover={s.turnover/10000000:.2f}Cr)"
                    for s in top_gainers
                )
            )
        if top_losers:
            logger.info(
                "Top Losers (selected): " + ", ".join(
                    f"{s.symbol}({s.change_pct:.2f}%, turnover={s.turnover/10000000:.2f}Cr)"
                    for s in top_losers
                )
            )

        try:
            cycles_total = int(getattr(self.settings, "cycles_per_stock", 3) or 3)
        except Exception:
            cycles_total = 3
        cycles_total = max(1, cycles_total)

        for stock in top_gainers:
            logger.info(f"Activating LONG: {stock.symbol} ({stock.change_pct:.2f}%)")
            stock.cycle_total = cycles_total
            stock.cycle_index = 0
            stock.cycle_start_mode = "LONG"
            self.start_long_ladder(stock)

        for stock in top_losers:
            logger.info(f"Activating SHORT: {stock.symbol} ({stock.change_pct:.2f}%)")
            stock.cycle_total = cycles_total
            stock.cycle_index = 0
            stock.cycle_start_mode = "SHORT"
            self.start_short_ladder(stock)

    def load_filtered_stocks(self, filepath: str = 'filtered_stocks.json') -> Dict[str, float]:
        """
        Load pre-filtered stocks from JSON file generated by premarket_filter.py.
        
        Args:
            filepath: Path to the filtered stocks JSON file
            
        Returns:
            Dictionary mapping symbols to their previous close prices
        """
        # Try Redis first (same-day cache)
        cached = load_candidates()
        if cached and cached.get("candidates"):
            current_count, current_hash = _stock_list_signature(STOCK_LIST)
            cached_count = cached.get("stock_list_count")
            cached_hash = cached.get("stock_list_hash")

            if cached_count is None or cached_hash is None:
                logger.warning(
                    "Ignoring legacy Redis cache for filtered stocks (missing stock-list signature). "
                    "Run 'python premarket_filter.py --force' to refresh."
                )
            else:
                try:
                    cached_count = int(cached_count)
                    cached_hash = str(cached_hash)
                except Exception:
                    cached_count, cached_hash = None, None

                if cached_count != current_count or cached_hash != current_hash:
                    logger.warning(
                        "Ignoring Redis cache for filtered stocks (STOCK_LIST signature mismatch). "
                        "Run 'python premarket_filter.py --force' to refresh."
                    )
                else:
                    stock_set = {str(s).strip().upper() for s in STOCK_LIST}
                    raw_candidates = cached.get("candidates", {}) or {}
                    candidates = {
                        str(sym).strip().upper(): float(prev_close)
                        for sym, prev_close in raw_candidates.items()
                        if str(sym).strip().upper() in stock_set
                    }
                    dropped = len(raw_candidates) - len(candidates)
                    if dropped:
                        logger.warning(
                            f"Redis cache contained {dropped} symbols not in current STOCK_LIST; ignoring them."
                        )
                    timestamp = cached.get("timestamp", "unknown")
                    logger.info(f"Loaded {len(candidates)} filtered stocks from Redis")
                    logger.info(f"Filter timestamp: {timestamp}")
                    return candidates

        if not os.path.exists(filepath):
            logger.error(f"Filtered stocks file not found: {filepath}")
            logger.error("Please run 'python premarket_filter.py' before starting the strategy engine")
            return {}
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            candidates = data.get('candidates', {})
            timestamp = data.get('timestamp', 'unknown')
            
            logger.info(f"Loaded {len(candidates)} filtered stocks from {filepath}")
            logger.info(f"Filter timestamp: {timestamp}")
            
            return candidates
            
        except Exception as e:
            logger.error(f"Error loading filtered stocks: {e}")
            return {}


    async def square_off_all(self, *, reason: str = "Emergency Square-off", final_status: str = "CLOSED_EMERGENCY"):
        """Emergency square-off all positions."""
        logger.warning(f"SQUARE OFF ALL triggered ({reason})")
        
        for stock in self.active_stocks.values():
            if stock.mode != "NONE" and stock.quantity > 0:
                self.close_position(stock, reason, final_status=final_status)
        
        logger.info("All positions squared off")

    def square_off_symbol(self, symbol: str, *, reason: str = "Manual Square-off", final_status: str = "CLOSED_MANUAL") -> bool:
        stock = self.active_stocks.get(symbol)
        if not stock:
            return False
        self.close_position(stock, reason, final_status=final_status)
        return True
