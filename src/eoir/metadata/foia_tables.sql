--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3 (Debian 15.3-1.pgdg110+1)
-- Dumped by pg_dump version 15.3 (Debian 15.3-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: foia_appeal; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_appeal_(%s)" (
    idnappeal integer NOT NULL,
    idncase integer,
    idnproceeding integer,
    strappealcategory text,
    strappealtype text,
    datappealfiled timestamp without time zone,
    strfiledby text,
    datattorneye27 timestamp without time zone,
    datbiadecision timestamp without time zone,
    strbiadecision text,
    strbiadecisiontype text,
    strcasetype text,
    strlang text,
    strnat text,
    strproceedingihp text,
    strcustody text,
    strprobono text
);


ALTER TABLE public."foia_appeal_(%s)" OWNER TO postgres;

--
-- Name: foia_application; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_application_(%s)" (
    idnproceedingappln integer NOT NULL,
    idnproceeding integer,
    idncase integer,
    appl_code text,
    appl_recd_date timestamp without time zone,
    appl_dec text
);


ALTER TABLE public."foia_application_(%s)" OWNER TO postgres;

--
-- Name: foia_bond; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_bond_(%s)" (
    idnassocbond integer,
    idnproceeding integer,
    idncase integer,
    osc_date timestamp without time zone,
    rec_type text,
    generation integer,
    sub_generation integer,
    update_date text,
    update_time time without time zone,
    input_date timestamp without time zone,
    input_time time without time zone,
    base_city_code text,
    base_city_name text,
    hearing_loc_code text,
    ij_code text,
    "dec" text,
    comp_date timestamp without time zone,
    initial_bond text,
    rel_con text,
    ins_ta text,
    bond_hearing_telephonic text,
    bond_hear_req_date timestamp without time zone,
    new_bond text,
    appeal_revd text,
    appeal_not_filed text,
    date_appeal_due timestamp without time zone,
    e_28_date timestamp without time zone,
    bond_type text,
    filing_method text,
    filing_party text,
    decision_due_date timestamp without time zone
);


ALTER TABLE public."foia_bond_(%s)" OWNER TO postgres;

--
-- Name: foia_case; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_case_(%s)" (
    idncase integer NOT NULL,
    alien_city text,
    alien_state text,
    alien_zipcode text,
    updated_zipcode text,
    updated_city text,
    nat text,
    lang text,
    custody text,
    site_type text,
    e_28_date timestamp without time zone,
    atty_nbr text,
    case_type text,
    updated_site text,
    latest_hearing timestamp without time zone,
    latest_time time without time zone,
    latest_cal_type text,
    up_bond_date timestamp without time zone,
    up_bond_rsn text,
    correctional_fac text,
    release_month text,
    release_year text,
    inmate_housing text,
    date_of_entry timestamp without time zone,
    c_asy_type text,
    c_birthdate text,
    c_release_date timestamp without time zone,
    updated_state text,
    address_changedon timestamp without time zone,
    zbond_mrg_flag text,
    gender text,
    date_detained timestamp without time zone,
    date_released timestamp without time zone,
    lpr text,
    detention_date timestamp without time zone,
    detention_location text,
    dco_location text,
    detention_facility_type text,
    casepriority_code text
);


ALTER TABLE public."foia_case_(%s)" OWNER TO postgres;

--
-- Name: foia_charges; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_charges_(%s)" (
    idnprcdchg integer NOT NULL,
    idncase integer,
    idnproceeding integer,
    charge text,
    chg_status text
);


ALTER TABLE public."foia_charges_(%s)" OWNER TO postgres;

--
-- Name: foia_motion; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_motion_(%s)" (
    idnmotion integer NOT NULL,
    idnproceeding integer,
    idncase integer,
    osc_date timestamp without time zone,
    rec_type text,
    generation text,
    sub_generation text,
    update_date timestamp without time zone,
    update_time time without time zone,
    input_date timestamp without time zone,
    input_time time without time zone,
    rej text,
    base_city_code text,
    hearing_loc_code text,
    ij_code text,
    ij_name text,
    "dec" text,
    comp_date timestamp without time zone,
    motion_recd_date timestamp without time zone,
    datmotiondue timestamp without time zone,
    wu_msg text,
    appeal_rsvd text,
    appeal_not_filed text,
    resp_due_date timestamp without time zone,
    stay_grant text,
    jurisdiction text,
    date_appeal_due timestamp without time zone,
    date_to_bia timestamp without time zone,
    decision_rendered timestamp without time zone,
    date_mailed_to_ij timestamp without time zone,
    date_recd_from_bia timestamp without time zone,
    date_to_bia_update integer,
    strfilingparty text,
    strfilingmethod text,
    strcertofservicecode text,
    e_28_recptflag bit varying,
    e_28_date timestamp without time zone,
    o_clock_option text,
    scheduled_hear_loc text,
    blndeleted text,
    strdjscenario text
);


ALTER TABLE public."foia_motion_(%s)" OWNER TO postgres;

--
-- Name: foia_proceeding; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_proceeding_(%s)" (
    idnproceeding integer NOT NULL,
    idncase integer,
    osc_date timestamp without time zone,
    input_date timestamp without time zone,
    base_city_code text,
    hearing_loc_code text,
    ij_code text,
    transfer_date timestamp without time zone,
    prev_hearing_loc text,
    prev_hearing_base text,
    prev_ij_code text,
    trans_nbr text,
    hearing_date timestamp without time zone,
    hearing_time time without time zone,
    dec_type text,
    dec_code text,
    dep1 text,
    dep2 text,
    other_comp text,
    appeal_rsvd text,
    appeal_not_filed text,
    comp_date timestamp without time zone,
    absentia text,
    venue_chg_granted timestamp without time zone,
    transfer_to text,
    date_appeal_due_status timestamp without time zone,
    transfer_status text,
    custody text,
    casetype text,
    nat text,
    lang text,
    scheduled_hear_loc text,
    correctional_fac text,
    crim_ind text,
    ihp text,
    aggravate_felon text,
    date_detained timestamp without time zone,
    date_released timestamp without time zone
);


ALTER TABLE public."foia_proceeding_(%s)" OWNER TO postgres;

--
-- Name: foia_rider; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_rider_(%s)" (
    idnleadrider integer NOT NULL,
    idnleadcase integer,
    idnridercase integer,
    datcreatedon timestamp without time zone,
    datmodifiedon timestamp without time zone,
    datseveredon timestamp without time zone,
    idnleadproceedingstart integer,
    idnleadproceedingend integer,
    idnriderproceedingstart integer,
    idnriderproceedingend integer,
    blnactive integer
);


ALTER TABLE public."foia_rider_(%s)" OWNER TO postgres;

--
-- Name: foia_schedule; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_schedule_(%s)" (
    idnschedule integer NOT NULL,
    idnproceeding integer,
    idncase integer,
    osc_date timestamp without time zone,
    generation integer,
    sub_generation integer,
    rec_type text,
    lang text,
    hearing_loc_code text,
    base_city_code text,
    ij_code text,
    interpreter_code text,
    input_date timestamp without time zone,
    input_time time without time zone,
    update_date timestamp without time zone,
    update_time time without time zone,
    assignment_path text,
    cal_type text,
    adj_date timestamp without time zone,
    adj_time_start time without time zone,
    adj_time_stop time without time zone,
    adj_rsn text,
    adj_med text,
    adj_msg text,
    adj_elap_days integer,
    lngsessnid integer,
    schedule_type text,
    notice_code text,
    blnclockoverride boolean,
    eoirattorneyid text
);


ALTER TABLE public."foia_schedule_(%s)" OWNER TO postgres;

--
-- Name: foia_atty; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_atty_(%s)" (
    eoirattorneyid text NOT NULL,
    oldattorneyid text,
    basecitycode text,
    blnattorneyactive integer,
    source_flag text,
    datcreatedon timestamp without time zone,
    datmodifiedon timestamp without time zone
);


ALTER TABLE public."foia_atty_(%s)" OWNER TO postgres;

--
-- Name: foia_caseid; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_caseid_(%s)" (
    idncaseid integer NOT NULL,
    idncase integer,
    case_id text
);


ALTER TABLE public."foia_caseid_(%s)" OWNER TO postgres;

--
-- Name: foia_casepriority; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_casepriority_(%s)" (
    idncaseprihistory integer NOT NULL,
    casepriority_code text,
    idncase integer,
    datcreatedon timestamp without time zone,
    datmodifiedon timestamp without time zone
);


ALTER TABLE public."foia_casepriority_(%s)" OWNER TO postgres;

--
-- Name: foia_custody; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_custody_(%s)" (
    idncustody integer NOT NULL,
    idncase integer,
    custody text,
    charge timestamp without time zone,
    chg_status timestamp without time zone
);


ALTER TABLE public."foia_custody_(%s)" OWNER TO postgres;

--
-- Name: foia_fedcourts; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_fedcourts_(%s)" (
    idnappealfedcourts integer NOT NULL,
    lngappealid integer,
    datrequestedbyoil timestamp without time zone,
    strfedcourtdecision text
);


ALTER TABLE public."foia_fedcourts_(%s)" OWNER TO postgres;

--
-- Name: foia_juvenile; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_juvenile_(%s)" (
    idnjuvenilehistory integer NOT NULL,
    idncase integer,
    idnproceeding integer,
    idnjuvenile integer,
    datcreatedon timestamp without time zone,
    datmodifiedon timestamp without time zone
);


ALTER TABLE public."foia_juvenile_(%s)" OWNER TO postgres;

--
-- Name: foia_probono; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_probono_(%s)" (
    case_type text,
    dec_212c text,
    dec_245 text,
    nbr_of_chgs text,
    other_dec1 text,
    other_dec2 text,
    p_eoir42a_dec text,
    p_eoir42b_dec text,
    vd_dec text,
    wd_dec text,
    strcorfacility text,
    dattranscriptservedalien timestamp without time zone,
    strproceedingihp text,
    strfiledby text,
    strnat text,
    strlang text,
    blnoarequestedbyalien integer,
    blnoarequestedbyins integer,
    blnoarequestedbyamicus integer,
    charge_1 text,
    charge_2 text,
    charge_3 text,
    charge_4 text,
    charge_5 text,
    charge_6 text,
    recd_212c text,
    recd_245 text,
    vd_recd text,
    wd_recd text,
    p_eoir42a_recd text,
    p_eoir42b_recd text,
    dec_code text,
    other_comp text,
    crim_ind text,
    stra1 text,
    stra2 text,
    stra3 text,
    stralienregion text,
    straliengender text,
    strinsstatus text,
    strpossibility text,
    datropreview timestamp without time zone,
    blnselectedbycoordinator integer,
    blnselectedbyscreener integer,
    optiona integer,
    dco_location text,
    caseid text,
    date_of_entry timestamp without time zone,
    inmate_housing text,
    datmailedtonfo timestamp without time zone,
    idnappeal integer,
    datbriefcurrentlyduealien timestamp without time zone,
    idnrepl integer NOT NULL,
    blnintrpr integer,
    strintprlang text,
    screeneridn integer,
    strdcaddress1 text,
    strdcaddress2 text,
    strdccity text,
    strdcstate text,
    strdczip text,
    blnprocessed integer,
    datcreatedon timestamp without time zone,
    datmodifiedon timestamp without time zone,
    strcustody text
);


ALTER TABLE public."foia_probono_(%s)" OWNER TO postgres;

--
-- Name: foia_reps; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_reps_(%s)" (
    idnrepsassigned integer NOT NULL,
    idncase integer,
    strattylevel text,
    strattytype text,
    parent_table text,
    parent_idn integer,
    base_city_code text,
    ins_ta_date_assigned timestamp without time zone,
    e_27_date timestamp without time zone,
    e_28_date timestamp without time zone,
    blnprimeatty boolean
);


ALTER TABLE public."foia_reps_(%s)" OWNER TO postgres;

--
-- Name: foia_threembr; Type: TABLE; Schema: public; Owner: postgres
--

CREATE UNLOGGED TABLE public."foia_threembr_(%s)" (
    idn3memberreferral integer NOT NULL,
    lngappealid integer,
    datreferredto3member timestamp without time zone,
    datremovedfromreferral timestamp without time zone
);


ALTER TABLE public."foia_threembr_(%s)" OWNER TO postgres;

