CREATE OR REPLACE PROCEDURE fill_w_zeros (IN in_zeros NUMERIC(3), IN in_value INTEGER, OUT out_value CHARACTER(99))
    LANGUAGE SQL MODIFIES SQL DATA
BEGIN
    SET out_value = in_value || '';
    WHILE LENGTH(TRIM(out_value)) < in_zeros  DO
        SET out_value = '0' || out_value;
    END WHILE;
END;


/**
  Ejecuta Programa de control vuelco de acuerdo a paralelización por cuenta cliente
  Parámetros:
  @in_emp: Empresa
  @in_bnjcod: Código de bandeja
  @in_mode: Modo 'C' para control, 'V' para vuelco

  Ejemplo:
  CALL submit_process(1,501,'C');
 */
CREATE OR REPLACE PROCEDURE submit_process (IN in_emp NUMERIC(3), IN in_bnjcod NUMERIC(4), IN in_mode CHARACTER(1))
    LANGUAGE SQL MODIFIES SQL DATA
BEGIN
    DECLARE v_Migsbcod INTEGER DEFAULT 0;
    DECLARE v_Migbacod INTEGER DEFAULT 0;
    DECLARE v_MIGBAPGC CHARACTER(10) DEFAULT '';
    DECLARE v_MIGBAPGV CHARACTER(10) DEFAULT '';
    DECLARE v_PROGRAM CHARACTER(10) DEFAULT '';
    DECLARE v_CMD CHARACTER(999) DEFAULT '';
    DECLARE v_STR_BNJCOD CHARACTER(10) DEFAULT '';
    DECLARE v_STR_BNJEMP CHARACTER(10) DEFAULT '';
    DECLARE v_STR_BNJSUC CHARACTER(10) DEFAULT '';
    DECLARE v_CMD_LEN DECIMAL(15,5) DEFAULT 0;

    SET v_Migsbcod = in_bnjcod / 100;
    SET v_Migbacod = in_bnjcod - (v_Migsbcod * 100);

    SELECT MIGBAPGC,MIGBAPGV INTO v_MIGBAPGC, v_MIGBAPGV
    FROM MIGBAND
    WHERE MIGBAEMP = in_emp AND MIGSBCOD = v_Migsbcod AND MIGBACOD = v_Migbacod;

    IF in_mode = 'C' THEN
        SET v_PROGRAM = v_MIGBAPGC;
    ELSEIF in_mode = 'V' THEN
        SET v_PROGRAM = v_MIGBAPGV;
    ELSE
        SIGNAL SQLSTATE '12345' SET MESSAGE_TEXT ='Invalid Parameter';
    END IF;

    CALL fill_w_zeros(3, in_emp, v_STR_BNJEMP);
    CALL fill_w_zeros(4, in_bnjcod, v_STR_BNJCOD);
    CALL fill_w_zeros(5, 0, v_STR_BNJSUC);
    FOR v AS BNJ021_C1 CURSOR FOR (SELECT BNJCLIDDE,BNJCLIHTA FROM BNJ021 Where BnjEmp = in_emp And BnjCod = in_bnjcod)
        DO
            SET v_CMD = 'SBMJOB CMD(CALL PGM(MIGRA_CP/' || TRIM(v_PROGRAM) || ') PARM(' ||
                        '''' || TRIM(v_STR_BNJEMP) || ''' ' ||
                        '''' || TRIM(v_STR_BNJCOD) || ''' ' ||
                        '''' || TRIM(v_STR_BNJSUC) || ''' ' ||
                        '''' || TRIM(v.BNJCLIDDE) || ''' ' ||
                        '''' || TRIM(v.BNJCLIHTA) || '''' ||
                        ')) JOB(DTORRES) JOBQ(QINTER)';
            SET v_CMD_LEN = LENGTH(TRIM(v_CMD));

            CALL QSYS.QCMDEXC(v_CMD, v_CMD_LEN);
    END FOR;
END;
