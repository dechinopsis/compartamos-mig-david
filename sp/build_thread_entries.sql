/**
  Paraleliza por cuenta cliente, operaciones registradas en BNJ002.
  Parámetros:
  @in_emp: Empresa
  @in_bnjcod: Código de bandeja
  @in_number_of_threads: Cantidad de vías

  Ejemplo:
  CALL build_thread_entries(1,501,20);
 */
CREATE OR REPLACE PROCEDURE build_thread_entries (in_emp NUMERIC(3), in_bnjcod NUMERIC(4), in_number_of_threads INT)
LANGUAGE SQL MODIFIES SQL DATA
BEGIN
    DECLARE v_open_current_thread SMALLINT DEFAULT 0;
    DECLARE v_total_operations INTEGER DEFAULT 0;
    DECLARE v_current_thread_operations INTEGER DEFAULT 0;
    DECLARE v_operations_per_thread INTEGER DEFAULT 0;
    DECLARE v_BnjCliDde NUMERIC(9) DEFAULT 0;
    DECLARE v_BnjCliHta NUMERIC(9) DEFAULT 0;
    DECLARE c_BnjSucPar NUMERIC(5) DEFAULT 99999;
    DECLARE c_BnjDocDde CHARACTER(25) DEFAULT '';
    DECLARE c_BnjDocHta CHARACTER(25) DEFAULT '';
    --END DECLARATIONS

    DELETE FROM BNJ021 WHERE BNJEMP = in_emp AND BNJCOD = in_bnjcod;
    SELECT COUNT(*) INTO v_total_operations FROM BNJ002 WHERE BNJEMP = in_emp AND BNJCOD = in_bnjcod;

    SET v_operations_per_thread = v_total_operations / in_number_of_threads;
    SET v_current_thread_operations = 0;
    SET v_BnjCliDde = 0;

    FOR v AS BNJ002_C1 CURSOR FOR (SELECT BnjCta, Count(*) AS RCOUNT From BNJ002 Where BnjEmp = in_emp And BnjCod = in_bnjcod GROUP By BnjCta ORDER BY BnjCta ASC)
        DO
            IF v_BnjCliDde = 0 THEN
                SET v_open_current_thread = 1;
                SET v_BnjCliDde = v.BnjCta;
            END IF;

            SET v_BnjCliHta = v.BnjCta;
            SET v_current_thread_operations = v_current_thread_operations + v.RCOUNT;

            IF v_current_thread_operations >= v_operations_per_thread THEN
                INSERT INTO BNJ021(BnjEmp, BnjCod, BnjSucPar, BnjCliDde, BnjCliHta, BnjDocDde, BnjDocHta)
                VALUES(in_emp, in_bnjcod, c_BnjSucPar, v_BnjCliDde, v_BnjCliHta, c_BnjDocDde , c_BnjDocHta);

                SET v_BnjCliDde = 0;
                SET v_current_thread_operations = 0;
                SET v_open_current_thread = 0;
            END IF;
    END FOR;

    IF v_open_current_thread = 1 THEN --Cursor ended and the last thread was not inserted.
        INSERT INTO BNJ021(BnjEmp, BnjCod, BnjSucPar, BnjCliDde, BnjCliHta, BnjDocDde, BnjDocHta)
        VALUES(in_emp, in_bnjcod, c_BnjSucPar, v_BnjCliDde, v_BnjCliHta, c_BnjDocDde, c_BnjDocHta);
    END IF;
END;