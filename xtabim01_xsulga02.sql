DROP TABLE Pobyt;
DROP TABLE Zákazník;
DROP TABLE Izba;
DROP TABLE Služba;
DROP MATERIALIZED VIEW recepciaView;

CREATE TABLE Izba (
    cisloIzby INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pocetLozok INT,
    cenaZaNoc INT,
    zoznamSluzieb VARCHAR(50) NULL,
    typ VARCHAR(20)
);

CREATE TABLE Služba (
    id INT PRIMARY KEY,
    nazov VARCHAR(30),
    cena INT,
    miesto VARCHAR(40) NULL,
    datumKonania TIMESTAMP
);

CREATE TABLE Zákazník (
    rodneCislo VARCHAR2(20) PRIMARY KEY CHECK (REGEXP_LIKE(rodneCislo, '[[:digit:]]+')),
    meno VARCHAR(50),
    priezvisko VARCHAR(50),
    telefonneCislo INT,
    email VARCHAR(100),
    cisloPreukazu INT,
    datumNarodenia TIMESTAMP
);

CREATE TABLE Pobyt (
    datumNastupu TIMESTAMP,
    cisloIzby INT REFERENCES Izba,
    rodneCislo VARCHAR2(20) REFERENCES Zákazník CHECK (REGEXP_LIKE(rodneCislo, '[[:digit:]]+')),
    PRIMARY KEY (datumNastupu, cisloIzby, rodneCislo),
    datumOdchodu TIMESTAMP NULL,
    typPlatby VARCHAR(20) NULL,
    cena INT,
    nahrada INT NULL,
    pocetNoci INT
);

CREATE OR REPLACE TRIGGER odstranenie_sluzby
	BEFORE DELETE ON SLUŽBA
	FOR EACH ROW
BEGIN
    UPDATE IZBA SET CENAZANOC = IZBA.CENAZANOC - :OLD.CENA
    WHERE REGEXP_LIKE(IZBA.ZOZNAMSLUZIEB, '' || :OLD.NAZOV || '');
    UPDATE IZBA SET ZOZNAMSLUZIEB = REGEXP_REPLACE(IZBA.ZOZNAMSLUZIEB,
                                                    '' || :OLD.NAZOV || '' ,
                                                    '');
END;
/

CREATE OR REPLACE TRIGGER zmena_ceny_izby
	BEFORE UPDATE OF CENAZANOC ON IZBA
	FOR EACH ROW
BEGIN
    UPDATE POBYT SET CENA = POBYT.CENA + (:NEW.CENAZANOC - :OLD.CENAZANOC) * POBYT.POCETNOCI
    WHERE POBYT.CISLOIZBY = :OLD.CISLOIZBY;
END;
/

CREATE OR REPLACE PROCEDURE zmena_miesta_salonu(noveMiesto IN STRING, noveKonanie IN DATE, stareMiesto IN STRING) IS
    CURSOR sluzba IS SELECT DISTINCT miesto, datumKonania FROM SLUŽBA;
    sluzba_zaznam sluzba%ROWTYPE;
    BEGIN
        OPEN sluzba;
        LOOP
            FETCH sluzba INTO sluzba_zaznam;
        EXIT WHEN sluzba%NOTFOUND;
        UPDATE Služba SET miesto = noveMiesto,
                          datumKonania = noveKonanie
            WHERE miesto = stareMiesto;
        END LOOP;
        CLOSE sluzba;
    END;
/

CREATE OR REPLACE PROCEDURE priemerny_prijem_zaObdobie (od IN TIMESTAMP, do IN TIMESTAMP) IS
    ziadnaCena EXCEPTION;
    CURSOR prijem IS SELECT SUM(cena) / COUNT(*) priemernaCena FROM Pobyt WHERE datumNastupu BETWEEN
    od AND do;
    prijem_cena prijem%ROWTYPE;
    BEGIN
        OPEN prijem;
        LOOP
            FETCH prijem INTO prijem_cena;
            EXIT WHEN prijem%NOTFOUND;
        IF prijem_cena.priemernaCena IS NULL THEN
            RAISE ziadnaCena;
        end if;
        DBMS_OUTPUT.PUT_LINE('Priemerna cena za obdobie:' || prijem_cena.priemernaCena);
        END LOOP;
        CLOSE prijem;
    EXCEPTION
        WHEN ziadnaCena THEN
            DBMS_OUTPUT.PUT_LINE('V danom obdobii neboli ziadny zakaznici.');
    END;
/

CREATE MATERIALIZED VIEW recepciaView AS
    SELECT cisloIzby, COUNT(*) pocet FROM Izba NATURAL JOIN Pobyt GROUP BY cisloIzby;

INSERT INTO Zákazník VALUES ('9109245', 'Jan', 'Panko', 0999111222, 'mail@mail.com', 100100, TO_DATE('10-10-1990', 'DD/MM/YYYY'));
INSERT INTO Zákazník VALUES ('11100010', 'Filip', 'Dobry', 0910100100, 'dobry@mail.com', 1019919, TO_DATE('01-01-1999', 'DD/MM/YYYY'));
INSERT INTO Zákazník VALUES ('01020304', 'Miro', 'Zly', 0902850195, 'mirko@mail.com', 1099556, TO_DATE('30-05-1965', 'DD/MM/YYYY'));
INSERT INTO Zákazník VALUES ('36021010', 'Darko', 'Nedarko', 421911180100, 'darko@mail.com', 9195467, TO_DATE('31-12-2000', 'DD/MM/YYYY'));

INSERT INTO Izba VALUES (DEFAULT, 4, 45, NULL, 'Standard');
INSERT INTO Izba VALUES (DEFAULT, 4, 45, NULL, 'Standard');
INSERT INTO Izba VALUES (DEFAULT, 4, 45, NULL, 'Standard');
INSERT INTO Izba VALUES (DEFAULT, 4, 45, NULL, 'Standard');
INSERT INTO Izba VALUES (DEFAULT, 2, 75, 'Virivka, sauna', 'Premium Deluxe');
INSERT INTO Izba VALUES (DEFAULT, 2, 75, 'Virivka, sauna', 'Premium Deluxe');
INSERT INTO Izba VALUES (DEFAULT, 2, 95, 'Virivka, sauna', 'Premium Deluxe');
INSERT INTO Izba VALUES (DEFAULT, 2, 95, 'Virivka, sauna', 'Premium Deluxe');

INSERT INTO Pobyt VALUES (TO_TIMESTAMP('30-03-2023 20:00:00', 'DD/MM/YYYY HH24:MI:SS'), 1, '9109245', NULL, NULL, 450, NULL, 5);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('25-04-2023 16:30:00', 'DD/MM/YYYY HH24:MI:SS'), 1, '9109245', TO_TIMESTAMP('28-04-2023 09:13:00', 'DD/MM/YYYY HH24:MI:SS'), 'Hotovost', 450, 120, 3);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('01-04-2022 19:00:00', 'DD/MM/YYYY HH24:MI:SS'), 3, '36021010', NULL, NULL, 450, NULL, 4);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('20-03-2022 16:00:00', 'DD/MM/YYYY HH24:MI:SS'), 7, '11100010', NULL, NULL, 450, NULL, 1);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('25-09-2023 18:00:00', 'DD/MM/YYYY HH24:MI:SS'), 8, '11100010', TO_TIMESTAMP('26-09-2023 11:46:56', 'DD/MM/YYYY HH24:MI:SS'), 'Kartou', 240, NULL, 1);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('26-09-2023 18:30:00', 'DD/MM/YYYY HH24:MI:SS'), 6, '11100010', TO_TIMESTAMP('28-09-2023 10:46:56', 'DD/MM/YYYY HH24:MI:SS'), 'Kartou', 240, NULL, 2);
INSERT INTO Pobyt VALUES (TO_TIMESTAMP('27-09-2023 19:00:00', 'DD/MM/YYYY HH24:MI:SS'), 5, '11100010', TO_TIMESTAMP('30-09-2023 09:46:56', 'DD/MM/YYYY HH24:MI:SS'), 'Kartou', 240, NULL, 3);

INSERT INTO Služba VALUES (1, 'Thajska masaz', 18, 'Salon c. 6', TO_TIMESTAMP('31-3-2023 17:45:00', 'DD/MM/YYYY HH24:MI:SS'));
INSERT INTO Služba VALUES (2, 'Ranajky do postele', 6, NULL, NULL);
INSERT INTO Služba VALUES (3, 'Vstup do bazena', 6, NULL, NULL);
INSERT INTO Služba VALUES (4, 'Virivka', 6, NULL, NULL);
INSERT INTO Služba VALUES (5, 'Sauna', 6, NULL, NULL);
INSERT INTO Služba VALUES (6, 'Pedikura', 10, 'Salon c. 1', TO_TIMESTAMP('21-10-2023 12:20:00', 'DD/MM/YYYY HH24:MI:SS'));


DELETE FROM SLUŽBA WHERE NAZOV = 'Virivka';

--najde vsetkych zakaznikov
--SELECT * FROM Zákazník;
--najde mena vsetkych zakaznikov ktori sa zucastnili nejakeho pobytu
--SELECT DISTINCT meno FROM Pobyt NATURAL JOIN Zákazník;
--najde vsetky izby na ktorych niekedy niekto byval
--SELECT DISTINCT cisloIzby FROM Pobyt NATURAL JOIN Izba GROUP BY cisloIzby;
--najde rodne cislo a meno tych ludi ktori boli ubytovani na izbe typu standard
--SELECT DISTINCT rodneCislo, meno FROM Pobyt NATURAL JOIN Izba NATURAL JOIN Zákazník WHERE typ = 'Standard';
--zisti kto sa zucastnil kolkych pobytov
--SELECT meno, COUNT(*) pocet FROM Zákazník NATURAL JOIN Pobyt GROUP BY meno;
--zisti na ktorej izbe bolo kolko pobytov
--SELECT cisloIzby, COUNT(*) pocet FROM Izba NATURAL JOIN Pobyt GROUP BY cisloIzby;
--najde vsetkych zakaznikov ktori boli ubytovani na izbe cislo 1 a nikde inde
--SELECT DISTINCT Z.rodneCislo,Z.meno,Z.priezvisko FROM Zákazník Z, Pobyt WHERE cisloIzby = 1 AND Z.rodneCislo=Pobyt.rodneCislo AND
--               NOT EXISTS(SELECT * FROM Pobyt WHERE Z.rodneCislo=Pobyt.rodneCislo AND cisloIzby <> 1);
--najde vsetky izby na ktore niekto nastupil na pobyt v roku 2023
--SELECT * FROM Izba WHERE cisloIzby IN (SELECT cisloIzby FROM Pobyt WHERE datumNastupu
--                BETWEEN TO_TIMESTAMP('01-01-2023 01:00:00', 'DD/MM/YYYY HH24:MI:SS') AND TO_TIMESTAMP('01-01-2024 01:00:00', 'DD/MM/YYYY HH24:MI:SS'));

EXPLAIN PLAN FOR
SELECT cisloIzby, COUNT(*) pocet FROM Izba NATURAL JOIN Pobyt GROUP BY cisloIzby;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

CREATE INDEX index1
ON POBYT (CISLOIZBY);

EXPLAIN PLAN FOR
SELECT cisloIzby, COUNT(*) pocet FROM Izba NATURAL JOIN Pobyt GROUP BY cisloIzby;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

WITH tab AS
(/*casedotaz*/
    SELECT MENO,COUNT(*) pocet FROM ZÁKAZNÍK NATURAL JOIN Pobyt GROUP BY MENO
)
SELECT MENO, pocet,
    CASE
        WHEN pocet < 5 and pocet > 2 THEN 5
        WHEN pocet < 10 and pocet > 5 THEN 10
        WHEN pocet < 15 and pocet > 10 THEN 15
        ELSE 0
    END discount
FROM tab;
--with vytvori |Zakaznik|pocet ubytovani|
--mozem pohadzat zakaznikov do kategorii podla toho kolko krat tu uz boli a dat im zlavu v case statemente

CALL zmena_miesta_salonu('Salon c. 3', TO_TIMESTAMP('31-03-2023 17:00:00', 'DD/MM/YYYY HH24:MI:SS'),'Salon c. 1');
CALL priemerny_prijem_zaObdobie(TO_TIMESTAMP('01-09-2025 00:00:00', 'DD/MM/YYYY HH24:MI:SS'), TO_TIMESTAMP('30-09-2025 23:59:59', 'DD/MM/YYYY HH24:MI:SS'));

GRANT ALL ON Pobyt TO XSULGA02;
GRANT ALL ON Zákazník TO XSULGA02;
GRANT ALL ON Izba TO XSULGA02;
GRANT ALL ON Služba TO XSULGA02;

GRANT ALL ON recepciaView TO XSULGA02;

COMMIT;