ALTER TABLE ONLY public."foia_appeal_(%s)"
    ADD CONSTRAINT foia_appeal_(%s)_pkey PRIMARY KEY (idnappeal);

ALTER TABLE ONLY public."foia_application_(%s)"
    ADD CONSTRAINT foia_application_(%s)_pkey PRIMARY KEY (idnproceedingappln);

ALTER TABLE ONLY public."foia_case_(%s)"
    ADD CONSTRAINT foia_case_(%s)_pkey PRIMARY KEY (idncase);

ALTER TABLE ONLY public."foia_charges_(%s)"
    ADD CONSTRAINT foia_charges_(%s)_pkey PRIMARY KEY (idnprcdchg);

ALTER TABLE ONLY public."foia_motion_(%s)"
    ADD CONSTRAINT foia_motion_(%s)_pkey PRIMARY KEY (idnmotion);

ALTER TABLE ONLY public."foia_proceeding_(%s)"
    ADD CONSTRAINT foia_proceeding_(%s)_pkey PRIMARY KEY (idnproceeding);

ALTER TABLE ONLY public."foia_rider_(%s)"
    ADD CONSTRAINT foia_rider_(%s)_pkey PRIMARY KEY (idnleadrider);

ALTER TABLE ONLY public."foia_schedule_(%s)"
    ADD CONSTRAINT foia_schedule_(%s)_pkey PRIMARY KEY (idnschedule);

ALTER TABLE ONLY public."foia_atty_(%s)"
    ADD CONSTRAINT foia_atty_(%s)_pkey PRIMARY KEY (eoirattorneyid);

ALTER TABLE ONLY public."foia_caseid_(%s)"
    ADD CONSTRAINT foia_caseid_(%s)_pkey PRIMARY KEY (idncaseid);

ALTER TABLE ONLY public."foia_casepriority_(%s)"
    ADD CONSTRAINT foia_casepriority_(%s)_pkey PRIMARY KEY (idncaseprihistory);

ALTER TABLE ONLY public."foia_custody_(%s)"
    ADD CONSTRAINT foia_custody_(%s)_pkey PRIMARY KEY (idncustody);

ALTER TABLE ONLY public."foia_fedcourts_(%s)"
    ADD CONSTRAINT foia_fedcourts_(%s)_pkey PRIMARY KEY (idnappealfedcourts);

ALTER TABLE ONLY public."foia_juvenile_(%s)"
    ADD CONSTRAINT foia_juvenile_(%s)_pkey PRIMARY KEY (idnjuvenilehistory);

ALTER TABLE ONLY public."foia_probono_(%s)"
    ADD CONSTRAINT foia_probono_(%s)_pkey PRIMARY KEY (idnrepl);

ALTER TABLE ONLY public."foia_reps_(%s)"
    ADD CONSTRAINT foia_reps_(%s)_pkey PRIMARY KEY (idnrepsassigned);

ALTER TABLE ONLY public."foia_threembr_(%s)"
    ADD CONSTRAINT foia_threembr_(%s)_pkey PRIMARY KEY (idn3memberreferral);

-- Indexes
CREATE INDEX ix_foia_appeal_(%s)_idncase ON public."foia_appeal_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_application_(%s)_idncase ON public."foia_application_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_atty_(%s)_eoirattorneyid ON public."foia_atty_(%s)" USING btree (eoirattorneyid);
CREATE INDEX ix_foia_bond_(%s)_idncase ON public."foia_bond_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_case_(%s)_idncase ON public."foia_case_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_caseid_(%s)_idncase ON public."foia_caseid_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_casepriority_(%s)_idncase ON public."foia_casepriority_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_charges_(%s)_idncase ON public."foia_charges_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_custody_(%s)_idncase ON public."foia_custody_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_fedcourts_(%s)_lngappealid ON public."foia_fedcourts_(%s)" USING btree (lngappealid);
CREATE INDEX ix_foia_juvenile_(%s)_idncase ON public."foia_juvenile_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_motion_(%s)_idncase ON public."foia_motion_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_proceeding_(%s)_idncase ON public."foia_proceeding_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_probono_(%s)_idnappeal ON public."foia_probono_(%s)" USING btree (idnappeal);
CREATE INDEX ix_foia_reps_(%s)_idncase ON public."foia_reps_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_rider_(%s)_leadcase ON public."foia_rider_(%s)" USING btree (idnleadcase);
CREATE INDEX ix_foia_rider_(%s)_ridercase ON public."foia_rider_(%s)" USING btree (idnridercase);
CREATE INDEX ix_foia_schedule_(%s)_idncase ON public."foia_schedule_(%s)" USING btree (idncase);
CREATE INDEX ix_foia_threembr_(%s)_lngappealid ON public."foia_threembr_(%s)" USING btree (lngappealid);

-- Foreign Keys
ALTER TABLE ONLY public."foia_application_(%s)"
    ADD CONSTRAINT foia_application_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_rider_(%s)"
    ADD CONSTRAINT foia_rider_(%s)_idnleadcase_fkey FOREIGN KEY (idnleadcase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_rider_(%s)"
    ADD CONSTRAINT foia_rider_(%s)_idnridercase_fkey FOREIGN KEY (idnridercase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_caseid_(%s)"
    ADD CONSTRAINT foia_caseid_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_casepriority_(%s)"
    ADD CONSTRAINT foia_casepriority_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_custody_(%s)"
    ADD CONSTRAINT foia_custody_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_juvenile_(%s)"
    ADD CONSTRAINT foia_juvenile_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_juvenile_(%s)"
    ADD CONSTRAINT foia_juvenile_(%s)_idnproceeding_fkey FOREIGN KEY (idnproceeding) REFERENCES public."foia_proceeding_(%s)"(idnproceeding);

ALTER TABLE ONLY public."foia_reps_(%s)"
    ADD CONSTRAINT foia_reps_(%s)_idncase_fkey FOREIGN KEY (idncase) REFERENCES public."foia_case_(%s)"(idncase);

ALTER TABLE ONLY public."foia_probono_(%s)"
    ADD CONSTRAINT foia_probono_(%s)_idnappeal_fkey FOREIGN KEY (idnappeal) REFERENCES public."foia_appeal_(%s)"(idnappeal);

ALTER TABLE ONLY public."foia_fedcourts_(%s)"
    ADD CONSTRAINT foia_fedcourts_(%s)_lngappealid_fkey FOREIGN KEY (lngappealid) REFERENCES public."foia_appeal_(%s)"(idnappeal);

ALTER TABLE ONLY public."foia_threembr_(%s)"
    ADD CONSTRAINT foia_threembr_(%s)_lngappealid_fkey FOREIGN KEY (lngappealid) REFERENCES public."foia_appeal_(%s)"(idnappeal);

-- Set Logged Tables
ALTER TABLE public."foia_appeal_(%s)" SET LOGGED;
ALTER TABLE public."foia_application_(%s)" SET LOGGED;
ALTER TABLE public."foia_atty_(%s)" SET LOGGED;
ALTER TABLE public."foia_bond_(%s)" SET LOGGED;
ALTER TABLE public."foia_case_(%s)" SET LOGGED;
ALTER TABLE public."foia_caseid_(%s)" SET LOGGED;
ALTER TABLE public."foia_casepriority_(%s)" SET LOGGED;
ALTER TABLE public."foia_charges_(%s)" SET LOGGED;
ALTER TABLE public."foia_custody_(%s)" SET LOGGED;
ALTER TABLE public."foia_fedcourts_(%s)" SET LOGGED;
ALTER TABLE public."foia_juvenile_(%s)" SET LOGGED;
ALTER TABLE public."foia_motion_(%s)" SET LOGGED;
ALTER TABLE public."foia_probono_(%s)" SET LOGGED;
ALTER TABLE public."foia_proceeding_(%s)" SET LOGGED;
ALTER TABLE public."foia_reps_(%s)" SET LOGGED;
ALTER TABLE public."foia_rider_(%s)" SET LOGGED;
ALTER TABLE public."foia_schedule_(%s)" SET LOGGED;
ALTER TABLE public."foia_threembr_(%s)" SET LOGGED;
