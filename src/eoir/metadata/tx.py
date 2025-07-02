def create_case_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.foia_case
        (
            idncase integer NOT NULL DEFAULT nextval('foia_case_idncase_seq'::regclass),
            alien_city text COLLATE pg_catalog."default",
            alien_state text COLLATE pg_catalog."default",
            alien_zipcode text COLLATE pg_catalog."default",
            updated_zipcode text COLLATE pg_catalog."default",
            updated_city text COLLATE pg_catalog."default",
            nat text COLLATE pg_catalog."default",
            lang text COLLATE pg_catalog."default",
            custody text COLLATE pg_catalog."default",
            site_type text COLLATE pg_catalog."default",
            e_28_date timestamp without time zone,
            atty_nbr text COLLATE pg_catalog."default",
            case_type text COLLATE pg_catalog."default",
            updated_site text COLLATE pg_catalog."default",
            latest_hearing timestamp without time zone,
            latest_time time without time zone,
            latest_cal_type text COLLATE pg_catalog."default",
            up_bond_date timestamp without time zone,
            up_bond_rsn text COLLATE pg_catalog."default",
            correctional_fac text COLLATE pg_catalog."default",
            release_month text COLLATE pg_catalog."default",
            release_year text COLLATE pg_catalog."default",
            inmate_housing text COLLATE pg_catalog."default",
            date_of_entry timestamp without time zone,
            c_asy_type text COLLATE pg_catalog."default",
            c_birthdate text COLLATE pg_catalog."default",
            c_release_date timestamp without time zone,
            updated_state text COLLATE pg_catalog."default",
            address_changedon timestamp without time zone,
            zbond_mrg_flag text COLLATE pg_catalog."default",
            gender text COLLATE pg_catalog."default",
            date_detained timestamp without time zone,
            date_released timestamp without time zone,
            lpr text COLLATE pg_catalog."default",
            detention_date timestamp without time zone,
            detention_location text COLLATE pg_catalog."default",
            dco_location text COLLATE pg_catalog."default",
            detention_facility_type text COLLATE pg_catalog."default",
            casepriority_code text COLLATE pg_catalog."default",
            CONSTRAINT foia_case_pkey PRIMARY KEY (idncase)
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public.foia_case
            OWNER to bklg;
        -- Index: ix_foia_case_idncase

        -- DROP INDEX IF EXISTS public.ix_foia_case_idncase;

        CREATE INDEX IF NOT EXISTS ix_foia_case_idncase
            ON public.foia_case USING btree
            (idncase ASC NULLS LAST)
            TABLESPACE pg_default;
    """
    )


def create_proceeding_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS public.foia_proceeding
        (
            idnproceeding integer NOT NULL DEFAULT nextval('foia_proceeding_idnproceeding_seq'::regclass),
            idncase integer,
            osc_date timestamp without time zone,
            input_date timestamp without time zone,
            base_city_code text COLLATE pg_catalog."default",
            hearing_loc_code text COLLATE pg_catalog."default",
            ij_code text COLLATE pg_catalog."default",
            transfer_date timestamp without time zone,
            prev_hearing_loc text COLLATE pg_catalog."default",
            prev_hearing_base text COLLATE pg_catalog."default",
            prev_ij_code text COLLATE pg_catalog."default",
            trans_nbr text COLLATE pg_catalog."default",
            hearing_date timestamp without time zone,
            hearing_time time without time zone,
            dec_type text COLLATE pg_catalog."default",
            dec_code text COLLATE pg_catalog."default",
            dep1 text COLLATE pg_catalog."default",
            dep2 text COLLATE pg_catalog."default",
            other_comp text COLLATE pg_catalog."default",
            appeal_rsvd text COLLATE pg_catalog."default",
            appeal_not_filed text COLLATE pg_catalog."default",
            comp_date timestamp without time zone,
            absentia text COLLATE pg_catalog."default",
            venue_chg_granted timestamp without time zone,
            transfer_to text COLLATE pg_catalog."default",
            date_appeal_due_status timestamp without time zone,
            transfer_status text COLLATE pg_catalog."default",
            custody text COLLATE pg_catalog."default",
            casetype text COLLATE pg_catalog."default",
            nat text COLLATE pg_catalog."default",
            lang text COLLATE pg_catalog."default",
            scheduled_hear_loc text COLLATE pg_catalog."default",
            correctional_fac text COLLATE pg_catalog."default",
            crim_ind text COLLATE pg_catalog."default",
            ihp text COLLATE pg_catalog."default",
            aggravate_felon text COLLATE pg_catalog."default",
            date_detained timestamp without time zone,
            date_released timestamp without time zone,
            CONSTRAINT foia_proceeding_pkey PRIMARY KEY (idnproceeding),
            CONSTRAINT foia_proceeding_idncase_fkey FOREIGN KEY (idncase)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        )

        TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS ix_foia_proceeding_idncase
            ON public.foia_proceeding USING btree
            (idncase ASC NULLS LAST)
            TABLESPACE pg_default;

        CREATE INDEX IF NOT EXISTS ix_foia_proceeding_idnproceeding
            ON public.foia_proceeding USING btree
            (idnproceeding ASC NULLS LAST)
            TABLESPACE pg_default;
    """
    )


def create_reps_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_reps;
        CREATE TABLE IF NOT EXISTS foia_reps (
            IDNREPSASSIGNED             INTEGER,
            IDNCASE                     INTEGER,
            STRATTYLEVEL                TEXT,
            STRATTYTYPE                 TEXT,
            PARENT_TABLE                TEXT,
            PARENT_IDN                  INTEGER,
            BASE_CITY_CODE              TEXT,
            INS_TA_DATE_ASSIGNED        TIMESTAMP,
            E_27_DATE                   TIMESTAMP,
            E_28_DATE                   TIMESTAMP,
            BLNPRIMEATTY                BOOLEAN,
            CONSTRAINT foia_reps_pkey PRIMARY KEY (IDNREPSASSIGNED),
            CONSTRAINT foia_reps_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );

        CREATE INDEX IF NOT EXISTS ix_foia_reps_idncase
            ON public.foia_reps USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_motion_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_motion;
        CREATE TABLE IF NOT EXISTS foia_motion (
            IDNMOTION               INTEGER,
            IDNPROCEEDING           INTEGER,
            IDNCASE                 INTEGER,
            OSC_DATE                TIMESTAMP,
            REC_TYPE                TEXT,
            GENERATION              TEXT,
            SUB_GENERATION          TEXT,
            UPDATE_DATE             TIMESTAMP,
            UPDATE_TIME             TIME,
            INPUT_DATE              TIMESTAMP,
            INPUT_TIME              TIME,
            REJ                     TEXT,
            BASE_CITY_CODE          TEXT,
            HEARING_LOC_CODE        TEXT,
            IJ_CODE                 TEXT,
            IJ_NAME                 TEXT,
            DEC                     TEXT,
            COMP_DATE               TIMESTAMP,
            MOTION_RECD_DATE        TIMESTAMP,
            DATMOTIONDUE            TIMESTAMP,
            WU_MSG                  TEXT,
            APPEAL_RSVD             TEXT,
            APPEAL_NOT_FILED        TEXT,
            RESP_DUE_DATE           TIMESTAMP,
            STAY_GRANT              TEXT,
            JURISDICTION            TEXT,
            DATE_APPEAL_DUE         TIMESTAMP,
            DATE_TO_BIA             TIMESTAMP,
            DECISION_RENDERED       TIMESTAMP,
            DATE_MAILED_TO_IJ       TIMESTAMP,
            DATE_RECD_FROM_BIA      TIMESTAMP,
            DATE_TO_BIA_UPDATE      INTEGER,
            STRFILINGPARTY          TEXT,
            STRFILINGMETHOD         TEXT,
            STRCERTOFSERVICECODE    TEXT,
            E_28_RECPTFLAG          TEXT,
            E_28_DATE               TIMESTAMP,
            O_CLOCK_OPTION          TEXT,
            SCHEDULED_HEAR_LOC      TEXT,
            BLNDELETED              TEXT,
            strDJScenario           TEXT,
            CONSTRAINT foia_motion_pkey PRIMARY KEY (IDNMOTION),
            CONSTRAINT foia_motion_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_motion_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );

        CREATE INDEX IF NOT EXISTS ix_foia_motion_idncase
            ON public.foia_motion USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_appeal_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_appeal;
        CREATE TABLE IF NOT EXISTS foia_appeal (
            idnAppeal               INTEGER,
            idncase                 INTEGER,
            idnProceeding           INTEGER,
            strAppealCategory       TEXT,
            strAppealType           TEXT,
            datAppealFiled          TIMESTAMP,
            strFiledBy              TEXT,
            datAttorneyE27          TIMESTAMP,
            datBIADecision          TIMESTAMP,
            strBIADecision          TEXT,
            strBIADecisionType      TEXT,
            strCaseType             TEXT,
            strLang                 TEXT,
            strNat                  TEXT,
            strProceedingIHP        TEXT,
            strCustody              TEXT,
            strProbono              TEXT,
            CONSTRAINT foia_appeal_pkey PRIMARY KEY (IDNAPPEAL),
            CONSTRAINT foia_appeal_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_appeal_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_appeal_idncase
            ON public.foia_appeal USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_appln_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_application;
        CREATE TABLE IF NOT EXISTS foia_application (
            IDNPROCEEDINGAPPLN      INTEGER,
            IDNPROCEEDING           INTEGER,
            IDNCASE                 INTEGER,
            APPL_CODE               TEXT,
            APPL_RECD_DATE          TIMESTAMP,
            APPL_DEC                TEXT,
            CONSTRAINT foia_appln_pkey PRIMARY KEY (IDNPROCEEDINGAPPLN),
            CONSTRAINT foia_appln_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_appln_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_appln_idncase
            ON public.foia_application USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_charges_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_charges;
        CREATE TABLE IF NOT EXISTS foia_charges (
            IDNPRCDCHG          INTEGER,
            IDNCASE             INTEGER,
            IDNPROCEEDING       INTEGER,
            CHARGE              TEXT,
            CHG_STATUS          TEXT,
            CONSTRAINT foia_charges_pkey PRIMARY KEY (IDNPRCDCHG),
            CONSTRAINT foia_charges_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_charges_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_charges_idncase
            ON public.foia_charges USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_bond_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS foia_bond (
            IDNASSOCBOND            INTEGER,
            IDNPROCEEDING           INTEGER,
            IDNCASE                 INTEGER,
            OSC_DATE                TIMESTAMP,
            REC_TYPE                TEXT,
            GENERATION              INTEGER,
            SUB_GENERATION          INTEGER,
            UPDATE_DATE             TEXT,
            UPDATE_TIME             TIME,
            INPUT_DATE              TIMESTAMP,
            INPUT_TIME              TIME,
            REJ                     TEXT,
            BASE_CITY_CODE          TEXT,
            BASE_CITY_NAME          TEXT,
            HEARING_LOC_CODE        TEXT,
            IJ_CODE                 TEXT,
            IJ_NAME                 TEXT,
            DEC                     TEXT,
            COMP_DATE               TIMESTAMP,
            INITIAL_BOND            TEXT,
            REL_CON                 TEXT,
            INS_TA                  TEXT,
            BOND_HEARING_TELEPHONIC TEXT,
            SEND_MSG_WU             TEXT,
            BOND_HEAR_REQ_DATE      TIMESTAMP,
            BOND_HEARING_DATE       TIMESTAMP,
            BOND_HEARING_TIME       TIME,
            ADJ1_CAL_TYPE           TEXT,
            ADJ1_DATE               TIMESTAMP,
            ADJ1_TIME               TIME,
            ADJ1_RSN                TEXT,
            ADJ1_TELEPHONIC         TEXT,
            ADJ1_MSG                TEXT,
            ADJ2_CAL_TYPE           TEXT,
            ADJ2_DATE               TIMESTAMP,
            ADJ2_TIME               TIME,
            ADJ2_RSN                TEXT,
            ADJ2_TELEPHONIC         TEXT,
            ADJ2_MSG                TEXT,
            NEW_BOND                TEXT,
            APPEAL_REVD             TEXT,
            APPEAL_NOT_FILED        TEXT,
            DATE_APPEAL_DUE         TIMESTAMP,
            E_28_DATE               TIMESTAMP,
            SCHEDULED_HEAR_LOC      TEXT,
            BOND_TYPE               TEXT,
            FILING_METHOD           TEXT,
            FILING_PARTY            TEXT,
            DECISION_DUE_DATE       TIMESTAMP,
            CONSTRAINT foia_bond_pkey PRIMARY KEY (IDNASSOCBOND),
            CONSTRAINT foia_bond_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_bond_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_bond_idncase
            ON public.foia_bond USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_custody_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_custody;
        CREATE TABLE IF NOT EXISTS foia_custody (
            IDNCUSTODY      INTEGER,
            IDNCASE         INTEGER,
            CUSTODY         TEXT,
            CHARGE          TIMESTAMP,
            CHG_STATUS      TIMESTAMP,
            CONSTRAINT foia_custody_pkey PRIMARY KEY (IDNCUSTODY),
            CONSTRAINT foia_custody_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_custody_idncase
            ON public.foia_custody USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_juvenile_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_juvenile;
        CREATE TABLE IF NOT EXISTS foia_juvenile (
            idnJuvenileHistory      INTEGER,
            idnCase                 INTEGER,
            idnProceeding           INTEGER,
            idnJuvenile             INTEGER,
            DATCREATEDON            TIMESTAMP,
            DATMODIFIEDON           TIMESTAMP,
            CONSTRAINT foia_juvenile_pkey PRIMARY KEY (idnJuvenileHistory),
            CONSTRAINT foia_juvenile_idncase_fkey FOREIGN KEY (idnCase)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_juvenile_idnproceeding_fkey FOREIGN KEY (IDNPROCEEDING)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_juvenile_idncase
            ON public.foia_juvenile USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_atty_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_atty;
        CREATE TABLE IF NOT EXISTS foia_atty (
            EOIRAttorneyID          TEXT,
            OldAttorneyID           TEXT,
            BaseCityCode            TEXT,
            blnAttorneyActive       INTEGER,
            Source_Flag             TEXT,
            datCreatedOn            TIMESTAMP,
            datModifiedOn           TIMESTAMP,
            CONSTRAINT foia_atty_pkey PRIMARY KEY (EOIRAttorneyID)
        );
        CREATE INDEX IF NOT EXISTS ix_foia_atty_eoirattorneyid
            ON public.foia_atty USING btree
            (eoirattorneyid ASC NULLS LAST);
    """
    )


def create_caseid_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_caseid;
        CREATE TABLE IF NOT EXISTS foia_caseid (
            IDNCASEID       INTEGER,
            IDNCASE         INTEGER,
            CASE_ID         TEXT,
            CONSTRAINT foia_caseid_pkey PRIMARY KEY (IDNCASEID),
            CONSTRAINT foia_caseid_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_caseid_idncase
            ON public.foia_caseid USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_casepriority_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_casepriority;
        CREATE TABLE IF NOT EXISTS foia_casepriority (
            idnCasePriHistory       INTEGER,
            casePriority_code       TEXT,
            idnCase                 INTEGER,
            DATCREATEDON            TIMESTAMP,
            DATMODIFIEDON           TIMESTAMP,
            CONSTRAINT foia_casepriority_pkey PRIMARY KEY (idncaseprihistory),
            CONSTRAINT foia_casepriority_idncase_fkey FOREIGN KEY (IDNCASE)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_casepriority_idncase
            ON public.foia_casepriority USING btree
            (idncase ASC NULLS LAST);
    """
    )


def create_rider_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_rider;
        CREATE TABLE IF NOT EXISTS foia_rider (
            idnLeadRider                INTEGER,
            idnLeadCase                 INTEGER,
            idnRiderCase                INTEGER,
            datCreatedOn                TIMESTAMP,
            datModifiedOn               TIMESTAMP,
            datSeveredOn                TIMESTAMP,
            idnLeadProceedingStart      INTEGER,
            idnLeadProceedingEnd        INTEGER,
            idnRiderProceedingStart     INTEGER,
            idnRiderProceedingEnd       INTEGER,
            blnActive                   INTEGER,
            CONSTRAINT foia_rider_pkey PRIMARY KEY (idnleadrider),
            CONSTRAINT foia_rider_idnleadcase_fkey FOREIGN KEY (idnLeadCase)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_rider_idnridercase_fkey FOREIGN KEY (idnRiderCase)
                REFERENCES public.foia_case (idncase) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_leadStart_idnproceeding_fkey FOREIGN KEY (idnLeadProceedingStart)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_leadEnd_idnproceeding_fkey FOREIGN KEY (idnLeadProceedingEnd)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_riderStart_idnproceeding_fkey FOREIGN KEY (idnRiderProceedingStart)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT foia_riderEnd_idnproceeding_fkey FOREIGN KEY (idnRiderProceedingEnd)
                REFERENCES public.foia_proceeding (IDNPROCEEDING) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_lead_idncase
            ON public.foia_rider USING btree
            (idnLeadCase ASC NULLS LAST);
        CREATE INDEX IF NOT EXISTS ix_foia_rider_idncase
            ON public.foia_rider USING btree
            (idnRiderCase ASC NULLS LAST);
    """
    )


def create_probono_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_probono;
        CREATE TABLE IF NOT EXISTS foia_probono (
            Case_type                       TEXT,
            DEC_212C                        TEXT,
            DEC_245                         TEXT,
            NBR_OF_CHGS                     TEXT,
            Other_dec1                      TEXT,
            Other_dec2                      TEXT,
            P_EOIR42A_DEC                   TEXT,
            P_EOIR42B_DEC                   TEXT,
            VD_DEC                          TEXT,
            WD_DEC                          TEXT,
            strCorFacility                  TEXT,
            datTranscriptServedAlien        TIMESTAMP,
            strProceedingIHP                TEXT,
            strFiledby                      TEXT,
            strNat                          TEXT,
            strLang                         TEXT,
            blnOARequestedbyAlien           INTEGER,
            blnOARequestedbyINS             INTEGER,
            blnOARequestedbyAmicus          INTEGER,
            Charge_1                        TEXT,
            Charge_2                        TEXT,
            Charge_3                        TEXT,
            Charge_4                        TEXT,
            Charge_5                        TEXT,
            Charge_6                        TEXT,
            recd_212C                       TEXT,
            recd_245                        TEXT,
            VD_recd                         TEXT,
            WD_recd                         TEXT,
            P_EOIR42A_Recd                  TEXT,
            P_EOIR42B_Recd                  TEXT,
            Dec_Code                        TEXT,
            other_comp                      TEXT,
            CRIM_IND                        TEXT,
            strA1                           TEXT,
            strA2                           TEXT,
            strA3                           TEXT,
            strAlienRegion                  TEXT,
            strAlienGender                  TEXT,
            strINSStatus                    TEXT,
            strPossibility                  TEXT,
            datROPreview                    TIMESTAMP,
            blnSelectedbyCoordinator        INTEGER,
            blnSelectedbyScreener           INTEGER,
            OptionA                         INTEGER,
            DCO_Location                    TEXT,
            CaseID                          TEXT,
            Date_Of_Entry                   TIMESTAMP,
            Inmate_Housing                  TEXT,
            datmailedtoNGO                  TIMESTAMP,
            idnAppeal                       INTEGER,
            datBriefCurrentlyDueAlien       TIMESTAMP,
            idnRepl                         INTEGER,
            blnIntrpr                       INTEGER,
            strIntrprLang                   TEXT,
            ScreenerIdn                     INTEGER,
            strDCAddress1                   TEXT,
            strDCAddress2                   TEXT,
            strDCCity                       TEXT,
            strDCState                      TEXT,
            strDCZip                        TEXT,
            blnProcessed                    INTEGER,
            datCreatedOn                    TIMESTAMP,
            datModifiedOn                   TIMESTAMP,
            strCustody                      TEXT,
            CONSTRAINT foia_probono_pkey PRIMARY KEY (idnRepl),
            CONSTRAINT foia_probono_idnappeal_fkey FOREIGN KEY (idnAppeal)
                REFERENCES public.foia_appeal (idnappeal) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
    """
    )


def create_fedcourts_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_fedcourts;
        CREATE TABLE IF NOT EXISTS foia_fedcourts (
            idnAppealFedCourts                      INTEGER,
            lngAppealID                             INTEGER,
            datRequestedByOIL                       TIMESTAMP,
            strFedCourtDecision                     TEXT,
            CONSTRAINT foia_fed_pkey PRIMARY KEY (idnAppealFedCourts),
            CONSTRAINT foia_fed_appeal_fkey FOREIGN KEY (lngAppealID)
                REFERENCES public.foia_appeal (idnappeal) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_fed_idnappeal
            ON public.foia_fedcourts USING btree
            (lngAppealID ASC NULLS LAST);
    """
    )


def create_threembr_table(cursor) -> None:
    cursor.execute(
        """
        DROP TABLE IF EXISTS foia_threembr;
        CREATE TABLE IF NOT EXISTS foia_threembr (
            idn3MemberReferral              INTEGER,
            lngAppealID                     INTEGER,
            datReferredTo3Member            TIMESTAMP,
            datRemovedFromReferral          TIMESTAMP,
            CONSTRAINT foia_threembr_pkey PRIMARY KEY (idn3MemberReferral),
            CONSTRAINT foia_threembr_appeal_fkey FOREIGN KEY (lngAppealID)
                REFERENCES public.foia_appeal (idnappeal) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        );
        CREATE INDEX IF NOT EXISTS ix_foia_threembr_idnappeal
            ON public.foia_threembr USING btree
            (lngAppealID ASC NULLS LAST);
    """
    )


create_tx_functions = {
    "foia_appeal": create_appeal_table,
    "appln": create_appln_table,
    "atty": create_atty_table,
    "bond": create_bond_table,
    "caseid": create_caseid_table,
    "casepriority": create_casepriority_table,
    "charges": create_charges_table,
    "custody": create_custody_table,
    "fedcourts": create_fedcourts_table,
    "juvenile": create_juvenile_table,
    "motion": create_motion_table,
    "probono": create_probono_table,
    "reps": create_reps_table,
    "rider": create_rider_table,
    "threembr": create_threembr_table,
}
