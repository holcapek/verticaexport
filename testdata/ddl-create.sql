----- stock-items.csv -----
CREATE TABLE stock_items (
	datum_vyst VARCHAR(255), -- convert to DATE
	typ_pohybu VARCHAR(255),
	sklad VARCHAR(255),
	kod_z_ceniku VARCHAR(255),
	nazev VARCHAR(255),
	prijem_mj VARCHAR(255), -- covnert to numeric(x,2)
	vydej_mj VARCHAR(255), -- covnert to numeric(x,2)
	pozadavek_mj VARCHAR(255), -- covnert to numeric(x,2)
	mnozstvi VARCHAR(255), -- covnert to numeric(x,2)
	cena_za_mj VARCHAR(255), -- covnert to numeric(x,2)
	celkem_kc VARCHAR(255), -- covnert to numeric(x,2)
	prijemka_vydejka VARCHAR(255),
	oznaceni VARCHAR(255),
	typ_ceny VARCHAR(255),
	kopirovat_predpis INT, -- bool?
	typ_polozky VARCHAR(255),
	firma_kod VARCHAR(255),
	firma_nazev VARCHAR(255),
	firma_ulice VARCHAR(255),
	firma_mesto VARCHAR(255),
	kopirovat_stredisko INT, -- bool?
	stredisko VARCHAR(255),
	mena VARCHAR(255),
	ucet_md_zakl INT,
	ucet_dal_zakl INT,
	skladova_karta VARCHAR(255),
	storno VARCHAR(255),
	vyr_cisla_ok INT,
	evidence_vyrobnich_cisel INT,
	zdroj_pro_faktury INT,
	cinnost INT
);

----- TV-Spots.csv -----

CREATE TABLE tv_spots (
	primary_key INT, -- is INT enough for 10000131825825248245 ? MAX_vint = 9223372036854775807
	date_time_start TIMESTAMP,
	date_time_end TIMESTAMP,
	footage INT, -- num seconds?
	day_of_spot VARCHAR(255),
	prime_time INT, -- or BOOL
	station VARCHAR(255),
	program_before VARCHAR(255),
	program_after VARCHAR(255),
	position VARCHAR(255),
	theme VARCHAR(255),
	trps NUMERIC(2,1),
	grps NUMERIC(2,1)
);
