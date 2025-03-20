import synoptic
import polars
import os, sys

allSites=dict(
	JuneauLand=['PAGY','PAHN','PAGS','PAJN','PAOH','SDIA2','PASI','PAGN','PAPG',
                'PAWG','PAKT','PAKW','PAHY','PAMM','PAFE'],
    JuneauMarine=['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                  'PGXA2','CDEA2','LCNA2','GIXA2','MXXA2','PBPA2','GLIA2','KEXA2',
                  'TKEA2'],
    JuneauMarineHRRR=['EROA2','LIXA2','NKXA2','SCXA2','RIXA2','CSPA2', 'FFIA2',
                      'PGXA2','CDXA2','LCNA2','GIXA2','JLXA2','PBPA2','GLIA2','KECA2',
                      'TKEA2'],
    AnchorageLand=['PANC','PAAQ','PAWS','PABV','SBPA2','PATO','WHMA2','PAWD',
                   'MCPA2','PAHO','PAEN','PASX','RUFA2','SHDA2','PAVD','RHVA2','RSCA2',
                   'PAGK','CXCA2','PXKA2','PLCA2','PATK','WOWA2','MMRA2','COVA2','PAII',
                   'PADL','PAPH','PADU','PAVC','PASD','UNLA2','NKLA2','PADK','PADQ',
                   'ALIA2','PANI','PAIG','PAIL','PASL','PABE','PAPM','PATG','PAKI',
                   'PAVA','PACV','ARCA2','SMCA2','PGPA2','RBTA2','GAHA2','RHRA2'],
    AnchorageMarine=['PAMD','46076','46061','46060','SELA2','PILA2','AMAA2','46080',
                     '46001','46078','SKJA2','46075','PAPB','PASN','BLIA2','AUGA2'],
    FairbanksLand=['ACRA2','BKCA2','TKRA2','CKNA2','SGNA2','PABI','FCPA2','TRDA2',
                   'TKLA2','PAIN','SMPA2','RXBA2','NHPA2','PAFA','TEXA2',
                   'PAEI','SLRA2','BCHA2','BREA2','PAFB','D6992','CPKA2','WICA2',
                   'PATA','PAEG','PFYU','PAGA','PABT','CHMA2','RAMA2','PAKP','IMYA2',
                   'PABA','PAAD','PASC','PAKU','PALP','PAQT','UMTA2','PAOR',
                   'PABR','PAWI','SSIA2','ASIA2','PAWN','PAOT','MNOA2','PAGH','SAGA2',
                   'PASK','PADE','HDOA2','PFSH','PFEL','PAKK','PAUN','PANV','PAOM',
                   'PATE','PATC','PAIW','PALU','PAPO','PAMK','PACZ','PASM','PADM',
                   'PATL','PAKV','PRDA2','PPIZ'])

df = synoptic.TimeSeries(
    stid="ukbkb",
    recent=timedelta(hours=96),
    vars="air_temp,dew_point_temperature",
    units="english",
).df()

