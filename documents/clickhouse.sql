create table visitor_stats(
                              stt DateTime,
                              edt DateTime,
                              vc String,
                              ch String,
                              ar String,
                              is_new String,
                              uv_ct UInt64,
                              pv_ct UInt64,
                              sv_ct UInt64,
                              uj_ct UInt64,
                              dur_sum UInt64,
                              ts UInt64
) engine = ReplacingMergeTree(ts)
partition by toYYYYMMDD(stt)
order by (stt,edt,is_new,vc,ch,ar);