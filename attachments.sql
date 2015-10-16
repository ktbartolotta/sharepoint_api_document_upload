-- Modified AI including status and date
with q as
(
    select a.ai_id, a.ai_parentai_id, type, facility, module_status, module_date,
        0 as Lvl, cast(row_number() over (order by a.ai_id) as varchar(MAX)) as bc
    from (
        select fn_id as ai_id, 'IS' as type, oshasite facility, statusds module_status,
            fn_assigneddt module_date, null as ai_parentai_id
        from impact.dbo.ppl_vw_talen_finding
        where fn_id in (
            select ai_parentai_id
            from impact.dbo.ppl_vw_talen_actionitem
        )
        union
        select ai_id, 'AI' as type, oshasiteds facility, statusds module_status,
            ai_createddt module_date, ai_parentai_id
        from impact.dbo.ppl_vw_talen_actionitem
        where ai_sourcekeyno is null
    ) a
    where ai_parentai_id is null
    union all
    select m.ai_id, m.ai_parentai_id, 'AI' as type, m.oshasiteds facility, m.statusds module_status,
            m.ai_createddt module_date, q.Lvl+1 as Lvl, q.bc
    from impact.dbo.ppl_vw_talen_actionitem m
        join q
            on m.ai_parentai_id = q.ai_id
),
h as  (
select b.ai_id, c.ai_id rootid, c.type rootcd, c.facility, b.module_status, b.module_date, b.lvl, b.bc
from  q b
    left join (
        select a.*
        from q a
        where a.ai_parentai_id is null
    ) c
        on b.bc = c.bc
)
select att.fileid, att.keyid, att.modulecd, roots.rootcd, roots.rootid, att.filename, att.description,
    att.data, att.dataisstoredyn, roots.is_restricted, roots.facility facility, roots.module_status, roots.module_date
from  impact.dbo.ppl_vw_talen_attachments att
    left join (
        -- AI
        (
            select att_ai1.fileid, att_ai1.modulecd, att_ai1.keyid, ai1.ai_source_modulecd rootcd, ai1.ai_sourcekeyno rootid,
                'N' is_restricted, ai1.oshasiteds facility, ai1.statusds module_status, ai1.ai_createddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_ai1
                inner join impact.dbo.ppl_vw_talen_actionitem ai1
                    on ai1.ai_id = att_ai1.keyid
            where att_ai1.modulecd = 'AI' and ai1.ai_source_modulecd = 'AU' and ai1.ai_sourcekeyno is not null
            union
            select att_ai2.fileid, att_ai2.modulecd, att_ai2.keyid, 'IN' rootcd, ai2.in_id rootid,
                case when ai2.ai_source_modulecd = 'ME' then 'Y' else 'N' end is_restricted,
                ai2.oshasiteds facility, ai2.statusds module_status, ai2.ai_createddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_ai2
                inner join impact.dbo.ppl_vw_talen_actionitem ai2
                    on ai2.ai_id = att_ai2.keyid
            where att_ai2.modulecd = 'AI' and ai2.in_id is not null
            union
            select att_ai3.fileid, att_ai3.modulecd, att_ai3.keyid, ai3.rootcd, ai3.rootid, 'N' is_restricted,
                ai3.facility, ai3.module_status, ai3.module_date
            from impact.dbo.ppl_vw_talen_attachments att_ai3
                inner join h ai3
                    on ai3.ai_id = att_ai3.keyid
            where att_ai3.modulecd = 'AI' and ai3.rootcd = 'AI'
            union (
                select att_ai4.fileid, att_ai4.modulecd, att_ai4.keyid, ai4.rootcd, ai4.rootid, 'N' is_restricted,
                    ai4.facility, ai4.module_status, ai4.module_date
                from impact.dbo.ppl_vw_talen_attachments att_ai4
                    inner join (
                        select h1.ai_id, h1.rootcd, h1.rootid, f1.oshasite facility,
                            h1.module_status, h1.module_date
                        from h h1
                            inner join impact.dbo.ppl_vw_talen_finding f1
                                on h1.rootid = f1.fn_id
                        where h1.rootcd = 'IS' and f1.fn_sourcekeyno is null
                        union
                        select h2.ai_id, 'AU' rootcd, f2.fn_sourcekeyno rootid, f2.oshasite facility,
                            h2.module_status, h2.module_date
                        from h h2
                            inner join impact.dbo.ppl_vw_talen_finding f2
                                on h2.rootid = f2.fn_id
                        where h2.rootcd = 'IS' and f2.fn_sourcekeyno is not null
                    ) ai4
                        on ai4.ai_id = att_ai4.keyid
                where att_ai4.modulecd = 'AI'
            )
            union
            select att_ai9.fileid, att_ai9.modulecd, att_ai9.keyid, att_ai9.modulecd rootcd, att_ai9.keyid rootid,
                'N' is_restricted, ai9.oshasiteds facility, ai9.statusds module_status, ai9.ai_createddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_ai9
                inner join impact.dbo.ppl_vw_talen_actionitem ai9
                    on ai9.ai_id = att_ai9.keyid
            where att_ai9.modulecd = 'AI' and ai9.ai_source_modulecd is null and ai9.ai_sourcekeyno is null and ai9.ai_parentai_id is null
        )
    ) roots
        on roots.fileid = att.fileid
where att.filename not like '%http://myccats/Impact/enterprise/review%'
    and att.modulecd = 'AI';



-- Modified Non-AI including status and date
select att.fileid, att.keyid, att.modulecd, roots.rootcd, roots.rootid, att.filename, att.description,
    att.data, att.dataisstoredyn, roots.is_restricted, roots.facility facility, roots.module_status, roots.module_date
from  impact.dbo.ppl_vw_talen_attachments att
    left join (
        select att_in.fileid, att_in.modulecd, att_in.keyid, att_in.modulecd rootcd, inc.in_id rootid, 'N' is_restricted,
            inc.regionds facility, inc.statusds module_status, inc.increateddt module_date
        from impact.dbo.ppl_vw_talen_attachments att_in
            inner join impact.dbo.ppl_vw_talen_incident inc
                on inc.in_id = att_in.keyid
        where att_in.modulecd = 'IN'
        union
        select att_au.fileid, att_au.modulecd, att_au.keyid,
            case when asmt.auditheadid is null then att_au.modulecd else att_au.modulecd end rootcd,
            case when asmt.auditheadid is null then att_au.keyid else asmt.auditheadid end rootid, 'N' is_restricted,
            asmt.oshasiteds facility, asmt.statusds module_status, asmt.au_auditdt module_date
        from impact.dbo.ppl_vw_talen_attachments att_au
            inner join impact.dbo.ppl_vw_talen_assessment asmt
                on asmt.auditheadid = att_au.keyid
        where att_au.modulecd = 'AU'
        union
        (
            select att_fn1.fileid, att_fn1.modulecd, att_fn1.keyid, att_fn1.modulecd rootcd, att_fn1.keyid rootid, 'N' is_restricted,
            fnd1.oshasite facility, fnd1.statusds module_status, fnd1.fn_assigneddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_fn1
                inner join impact.dbo.ppl_vw_talen_finding fnd1
                    on fnd1.fn_id = att_fn1.keyid
            where att_fn1.modulecd = 'IS' and fnd1.fn_sourcekeyno is null
            union
            select att_fn2.fileid, att_fn2.modulecd, att_fn2.keyid, fnd2.fn_source_modulecd rootcd, fnd2.fn_sourcekeyno rootid,
                'N' is_restricted, fnd2.oshasite facility, fnd2.statusds module_status, fnd2.fn_assigneddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_fn2
                inner join impact.dbo.ppl_vw_talen_finding fnd2
                    on fnd2.fn_id = att_fn2.keyid
            where att_fn2.modulecd = 'IS' and fnd2.fn_sourcekeyno is not null
        )
        union
        select att_ve.fileid, att_ve.modulecd, att_ve.keyid, 'IN' rootcd, veh.in_id rootid, 'N' is_restricted,
            veh.regionds facility, veh.statusds module_status, veh.createddt module_date
        from impact.dbo.ppl_vw_talen_attachments att_ve
            inner join impact.dbo.ppl_vw_talen_vehicle veh
                on veh.ve_id = att_ve.keyid
        where att_ve.modulecd = 'VE'
        union
        select att_qe.fileid, att_qe.modulecd, att_qe.keyid, 'IN' rootcd, gen.in_id rootid, 'N' is_restricted,
            gen.oshasite facility, gen.statusds module_status, gen.reporteddt module_date
        from impact.dbo.ppl_vw_talen_attachments att_qe
            inner join impact.dbo.ppl_vw_talen_generation gen
                on gen.qe_id = att_qe.keyid
        where att_qe.modulecd = 'QE'
        union
        select att_me.fileid, att_me.modulecd, att_me.keyid,
            case when med.in_id is null then att_me.modulecd else 'IN' end rootcd,
            case when med.in_id is null then att_me.keyid else med.in_id end rootid, 'Y' is_restricted,
            med.oshasite facility, med.statusds module_status, med.incidentdt module_date
        from impact.dbo.ppl_vw_talen_attachments att_me
            inner join impact.dbo.ppl_vw_talen_medical med
                on med.me_id = att_me.keyid
        where att_me.modulecd = 'ME'
        union
        select att_ee.fileid, att_ee.modulecd, att_ee.keyid, 'IN' rootcd, env.in_id rootid, 'N' is_restricted,
            env.oshasite facility, env.statusds module_status, env.incidentdt module_date
        from impact.dbo.ppl_vw_talen_attachments att_ee
            inner join impact.dbo.ppl_vw_talen_environmental env
                on env.ee_id = att_ee.keyid
        where att_ee.modulecd = 'EE'
        union
        select att_pe.fileid, att_pe.modulecd, att_pe.keyid, 'IN' rootcd, pro.in_id rootid, 'N' is_restricted,
            pro.oshasite facility, pro.statusds module_status, pro.incidentdt module_date
        from impact.dbo.ppl_vw_talen_attachments att_pe
            left join impact.dbo.ppl_vw_talen_property pro
                on pro.pe_id = att_pe.keyid
        where att_pe.modulecd = 'PE'
        union
        (
            select att_iv1.fileid, att_iv1.modulecd, att_iv1.keyid, att_iv1.modulecd rootcd, inv1.iv_id rootid, 'N' is_restricted,
                inv1.osha_site facility, inv1.statusds module_status, inv1.iv_conducteddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_iv1
                left join impact.dbo.ppl_vw_talen_investigation inv1
                    on inv1.iv_id = att_iv1.keyid
            where att_iv1.modulecd = 'IV' and inv1.in_id is null
            union
            select att_iv2.fileid, att_iv2.modulecd, att_iv2.keyid, 'IN' rootcd, inv2.in_id rootid, 'N' is_restricted,
                inv2.osha_site facility, inv2.statusds module_status, inv2.iv_conducteddt module_date
            from impact.dbo.ppl_vw_talen_attachments att_iv2
                inner join impact.dbo.ppl_vw_talen_investigation inv2
                    on inv2.iv_id = att_iv2.keyid
            where att_iv2.modulecd = 'IV' and inv2.in_id is not null
        )
    ) roots
        on roots.fileid = att.fileid
where att.filename not like '%http://myccats/Impact/enterprise/review%'
    where att.modulecd != 'AI';
