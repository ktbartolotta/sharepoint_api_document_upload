import pyodbc
import pprint
import requests
import getpass
import traceback
import sys
import re
import os

from requests_ntlm import HttpNtlmAuth
from collections import namedtuple
from string import printable


AttMetadata = namedtuple(
    'AttMetadata',
    ['fileid', 'modulecd', 'keyid', 'rootcd', 'rootid', 'filename',
     'description', 'data', 'dataisstoredyn', 'is_restricted', 'facility'])


def get_ai_attachments_query():

    query = """
        with q as
        (
            select a.ai_id, a.ai_parentai_id, type, facility, 0 as Lvl, cast(row_number() over (order by a.ai_id) as varchar(MAX)) as bc
            from (
                select fn_id as ai_id, 'IS' as type, oshasite facility, null as ai_parentai_id
                from impact.dbo.ppl_vw_talen_finding
                where fn_id in (
                    select ai_parentai_id
                    from impact.dbo.ppl_vw_talen_actionitem
                )
                union
                select ai_id, 'AI' as type, oshasiteds facility, ai_parentai_id
                from impact.dbo.ppl_vw_talen_actionitem
                where ai_sourcekeyno is null
            ) a
            where ai_parentai_id is null
            union all
            select m.ai_id, m.ai_parentai_id, 'AI' as type, m.oshasiteds facility, q.Lvl+1 as Lvl, q.bc
            from impact.dbo.ppl_vw_talen_actionitem m
                join q
                    on m.ai_parentai_id = q.ai_id
        ),
        h as  (
        select b.ai_id, c.ai_id rootid, c.type rootcd, c.facility, b.lvl, b.bc
        from  q b
            left join (
                select a.*
                from q a
                where a.ai_parentai_id is null
            ) c
                on b.bc = c.bc
        )
        select att.fileid, att.modulecd, att.keyid, roots.rootcd, roots.rootid, att.filename, att.description,
            att.data, att.dataisstoredyn, roots.is_restricted, roots.facility facility
        from  impact.dbo.ppl_vw_talen_attachments att
            inner join (
                -- AI
                (
                    select att_ai1.fileid, att_ai1.modulecd, att_ai1.keyid, ai1.ai_source_modulecd rootcd, ai1.ai_sourcekeyno rootid, 
                        'N' is_restricted, ai1.oshasiteds facility
                    from impact.dbo.ppl_vw_talen_attachments att_ai1
                        inner join impact.dbo.ppl_vw_talen_actionitem ai1
                            on ai1.ai_id = att_ai1.keyid
                    where att_ai1.modulecd = 'AI' and ai1.ai_source_modulecd = 'AU' and ai1.ai_sourcekeyno is not null
                    union
                    select att_ai2.fileid, att_ai2.modulecd, att_ai2.keyid, 'IN' rootcd, ai2.in_id rootid,
                        case when ai2.ai_source_modulecd = 'ME' then 'Y' else 'N' end is_restricted,
                        ai2.oshasiteds facility
                    from impact.dbo.ppl_vw_talen_attachments att_ai2
                        inner join impact.dbo.ppl_vw_talen_actionitem ai2
                            on ai2.ai_id = att_ai2.keyid
                    where att_ai2.modulecd = 'AI' and ai2.in_id is not null
                    union
                    select att_ai3.fileid, att_ai3.modulecd, att_ai3.keyid, ai3.rootcd, ai3.rootid, 'N' is_restricted,
                        ai3.facility
                    from impact.dbo.ppl_vw_talen_attachments att_ai3
                        inner join h ai3
                            on ai3.ai_id = att_ai3.keyid
                    where att_ai3.modulecd = 'AI' and ai3.rootcd = 'AI'
                    union (
                        select att_ai4.fileid, att_ai4.modulecd, att_ai4.keyid, ai4.rootcd, ai4.rootid, 'N' is_restricted,
                            ai4.facility
                        from impact.dbo.ppl_vw_talen_attachments att_ai4
                            inner join (
                                select h1.ai_id, h1.rootcd, h1.rootid, f1.oshasite facility
                                from h h1
                                    inner join impact.dbo.ppl_vw_talen_finding f1
                                        on h1.rootid = f1.fn_id
                                where h1.rootcd = 'IS' and f1.fn_sourcekeyno is null
                                union
                                select h2.ai_id, 'AU' rootcd, f2.fn_sourcekeyno rootid, f2.oshasite facility
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
                        'N' is_restricted, ai9.oshasiteds facility
                    from impact.dbo.ppl_vw_talen_attachments att_ai9
                        inner join impact.dbo.ppl_vw_talen_actionitem ai9
                            on ai9.ai_id = att_ai9.keyid
                    where att_ai9.modulecd = 'AI' and ai9.ai_source_modulecd is null and ai9.ai_sourcekeyno is null and ai9.ai_parentai_id is null
                )
            ) roots
                on roots.fileid = att.fileid
        where att.filename not like '%http://myccats/Impact/enterprise/review%'"""


    return query


def get_non_ai_attachments_query():

    query = """
        select att.fileid, att.modulecd, att.keyid, roots.rootcd, roots.rootid, att.filename, att.description,
            att.data, att.dataisstoredyn, roots.is_restricted, roots.facility facility
        from  impact.dbo.ppl_vw_talen_attachments att
            inner join (
                select att_in.fileid, att_in.modulecd, att_in.keyid, att_in.modulecd rootcd, inc.in_id rootid, 'N' is_restricted,
                    inc.regionds facility
                from impact.dbo.ppl_vw_talen_attachments att_in
                    left join impact.dbo.ppl_vw_talen_incident inc
                        on inc.in_id = att_in.keyid
                where att_in.modulecd = 'IN'
                union
                select att_au.fileid, att_au.modulecd, att_au.keyid,
                    case when asmt.auditheadid is null then att_au.modulecd else att_au.modulecd end rootcd,
                    case when asmt.auditheadid is null then att_au.keyid else asmt.auditheadid end rootid, 'N' is_restricted,
                    asmt.oshasiteds facility
                from impact.dbo.ppl_vw_talen_attachments att_au
                    left join impact.dbo.ppl_vw_talen_assessment asmt
                        on asmt.auditheadid = att_au.keyid
                where att_au.modulecd = 'AU'
                union
                (
                    select att_fn1.fileid, att_fn1.modulecd, att_fn1.keyid, att_fn1.modulecd rootcd, att_fn1.keyid rootid, 'N' is_restricted,
                    fnd1.oshasite facility
                    from impact.dbo.ppl_vw_talen_attachments att_fn1
                        left join impact.dbo.ppl_vw_talen_finding fnd1
                            on fnd1.fn_id = att_fn1.keyid
                    where att_fn1.modulecd = 'IS' and fnd1.fn_sourcekeyno is null
                    union
                    select att_fn2.fileid, att_fn2.modulecd, att_fn2.keyid, fnd2.fn_source_modulecd rootcd, fnd2.fn_sourcekeyno rootid, 
                        'N' is_restricted, fnd2.oshasite facility
                    from impact.dbo.ppl_vw_talen_attachments att_fn2
                        left join impact.dbo.ppl_vw_talen_finding fnd2
                            on fnd2.fn_id = att_fn2.keyid
                    where att_fn2.modulecd = 'IS' and fnd2.fn_sourcekeyno is not null
                )
                union
                select att_ve.fileid, att_ve.modulecd, att_ve.keyid, 'IN' rootcd, veh.in_id rootid, 'N' is_restricted,
                    veh.regionds facility
                from impact.dbo.ppl_vw_talen_attachments att_ve
                    left join impact.dbo.ppl_vw_talen_vehicle veh
                        on veh.ve_id = att_ve.keyid
                where att_ve.modulecd = 'VE'
                union
                select att_qe.fileid, att_qe.modulecd, att_qe.keyid, 'IN' rootcd, gen.in_id rootid, 'N' is_restricted,
                    gen.oshasite facility
                from impact.dbo.ppl_vw_talen_attachments att_qe
                    left join impact.dbo.ppl_vw_talen_generation gen
                        on gen.qe_id = att_qe.keyid
                where att_qe.modulecd = 'QE'
                union
                select att_me.fileid, att_me.modulecd, att_me.keyid,
                    case when med.in_id is null then att_me.modulecd else 'IN' end rootcd,
                    case when med.in_id is null then att_me.keyid else med.in_id end rootid, 'Y' is_restricted,
                    med.oshasite facility
                from impact.dbo.ppl_vw_talen_attachments att_me
                    left join impact.dbo.ppl_vw_talen_medical med
                        on med.me_id = att_me.keyid
                where att_me.modulecd = 'ME'
                union
                select att_ee.fileid, att_ee.modulecd, att_ee.keyid, 'IN' rootcd, env.in_id rootid, 'N' is_restricted,
                    env.oshasite facility
                from impact.dbo.ppl_vw_talen_attachments att_ee
                    left join impact.dbo.ppl_vw_talen_environmental env
                        on env.ee_id = att_ee.keyid
                where att_ee.modulecd = 'EE'
                union
                select att_pe.fileid, att_pe.modulecd, att_pe.keyid, 'IN' rootcd, pro.in_id rootid, 'N' is_restricted,
                    pro.oshasite facility
                from impact.dbo.ppl_vw_talen_attachments att_pe
                    left join impact.dbo.ppl_vw_talen_property pro
                        on pro.pe_id = att_pe.keyid
                where att_pe.modulecd = 'PE'
                union
                (
                    select att_iv1.fileid, att_iv1.modulecd, att_iv1.keyid, att_iv1.modulecd rootcd, inv1.iv_id rootid, 'N' is_restricted,
                        inv1.osha_site facility
                    from impact.dbo.ppl_vw_talen_attachments att_iv1
                        left join impact.dbo.ppl_vw_talen_investigation inv1
                            on inv1.iv_id = att_iv1.keyid
                    where att_iv1.modulecd = 'IV' and inv1.in_id is null
                    union
                    select att_iv2.fileid, att_iv2.modulecd, att_iv2.keyid, 'IN' rootcd, inv2.in_id rootid, 'N' is_restricted,
                        inv2.osha_site facility
                    from impact.dbo.ppl_vw_talen_attachments att_iv2
                        left join impact.dbo.ppl_vw_talen_investigation inv2
                            on inv2.iv_id = att_iv2.keyid
                    where att_iv2.modulecd = 'IV' and inv2.in_id is not null
                )
            ) roots
                on roots.fileid = att.fileid
        where att.filename not like '%http://myccats/Impact/enterprise/review%'"""

    return query


def get_attachments(query):
    """
    Execute attachments query and yield Attachment data."""
    try:
        cnn_str = ";".join([
            "Driver={SQL Server}", "Server=WIN-DBFM-G-100\\INST05",
            "Trusted_Connection=yes", "database=Impact"
        ])
        cnn = pyodbc.connect(cnn_str)
        cur = cnn.cursor()
        cur.execute(query)
        for rec in cur:
            yield AttMetadata(
                fileid=unicode(rec[0]),
                modulecd=unicode(rec[1]),
                keyid=unicode(rec[2]),
                rootcd=unicode(rec[3] if rec[3] else rec[1]),
                rootid=unicode(rec[4] if rec[4] else rec[2]),
                filename=filter(lambda s: s in printable, rec[5])
                if rec[5] else u'No file name',
                description=filter(lambda s: s in printable, rec[6])
                if rec[6] else u'No description.',
                data=rec[7],
                dataisstoredyn=unicode(rec[8]),
                is_restricted=unicode(rec[9]),
                facility=unicode(rec[10])
            )
    except:
        traceback.print_exc(file=sys.stdout)
    finally:
        cur.close()
        cnn.close()


def get_auth():
    """
    Request Windows credentials from user.  Used in Sharepoint site access.
    User must have at least Member role."""
    user_name = raw_input("username:")
    password = getpass.getpass("password:")
    return HttpNtlmAuth("PPL\\%s" % user_name, password)


def get_api_url():

    return "http://engagesites.ppl.com/energysupply/gtspc/_api"


def get_x_request_digest(auth):
    """
    Extract X-RequestDigest value from site."""
    try:
        url = "/".join([get_api_url(), "contextinfo"])
        headers = {
            "accept": "application/json;odata=verbose"
        }
        response = requests.post(
            url=url,
            auth=auth,
            headers=headers)
        print('X-RequestDigest:  %s' % response.status_code)
        return response.json()['d']['GetContextWebInformation']['FormDigestValue']
    except Exception, e:
        print(response.json())
        raise e


# def upload_binary(auth, xrd, library, folder, filename, file_data):
#     """
#     Upload binary file data to Sharepoint document library."""
#     try:
#         response = requests.post(
#             url="/".join([
#                 get_api_url(), "web",
#                 "GetFolderByServerRelativeUrl('%s/%s')",
#                 "files",
#                 "add(url='%s',overwrite=true)"
#             ]) % (library, folder, filename),
#             auth=auth,
#             headers={
#                 "accept": "application/json;odata=verbose",
#                 "content-type": "application/x-www-urlencoded; charset=UTF-8",
#                 "content-length": len(file_data),
#                 "X-RequestDigest": xrd
#             },
#             data=file_data)
#         print('Upload:  %s' % response.status_code)
#         if '20' not in str(response.status_code):
#             print('Retrying connection')
#             upload_binary(auth, xrd, library, folder, filename, file_data)
#         return response.json()['d']['ListItemAllFields']['__deferred']['uri']
#     except Exception, e:
#         print(response.json())
#         raise e


def upload_binary(auth, xrd, library, filename, file_data):
    """
    Upload binary file data to Sharepoint document library."""
    try:
        response = requests.post(
            url="/".join([
                get_api_url(), "web",
                "GetFolderByServerRelativeUrl('%s')",
                "files",
                "add(url='%s',overwrite=true)"
            ]) % (library, filename),
            auth=auth,
            headers={
                "accept": "application/json;odata=verbose",
                "content-type": "application/x-www-urlencoded; charset=UTF-8",
                "content-length": len(file_data),
                "X-RequestDigest": xrd
            },
            data=file_data)
        print('Upload:  %s' % response.status_code)
        if '20' not in str(response.status_code):
            print('Retrying connection')
            upload_binary(auth, xrd, library, filename, file_data)
        return response.json()['d']['ListItemAllFields']['__deferred']['uri']
    except Exception, e:
        print(response.json())
        raise e


# def create_folder(auth, xrd, library, folder):
#     """
#     Create sub-folder."""
#     try:
#         data = """
#             {'__metadata': {'type': 'SP.Folder'},
#             'ServerRelativeUrl': '%s/%s'}""" % (library, folder)

#         response = requests.post(
#             url="/".join([get_api_url(), "web", "folders"]),
#             auth=auth,
#             headers={
#                 "accept": "application/json;odata=verbose",
#                 "content-type": "application/json;odata=verbose",
#                 "content-length": len(data),
#                 "X-RequestDigest": xrd
#             },
#             data=data)
#         print('Create Folder:  %s' % response.status_code)
#     except Exception, e:
#         print(response.json())
#         raise e


def get_item_metadata(auth, item_fields_uri):
    """
    Extract item uri, etag, and list item type."""
    try:
        response = requests.get(
            url=item_fields_uri,
            auth=auth,
            headers={
                "accept": "application/json;odata=verbose"
            }
        )
        list_resp = response.json()
        print('Metadata:  %s' % response.status_code)

        return {
            'uri': list_resp['d']['__metadata']['uri'],
            'etag': list_resp['d']['__metadata']['etag'],
            'type': list_resp['d']['__metadata']['type']
        }
    except Exception, e:
        print(response.json())
        raise e


def update_file_item(auth, xrd, item_metadata, item_data):
    """
    Update the metadata fields for an item."""
    try:
        bad_desc_match = re.compile("[\'\"\\\/]")
        data = """
            {'__metadata': {'type': '%s'},
             'fileid': '%s',
             'modkey': '%s',
             'description0': '%s',
             'rootkey': '%s',
             'facility': '%s'}""" % (
                item_metadata['type'],
                item_data.fileid,
                "_".join([item_data.modulecd, item_data.keyid]),
                bad_desc_match.sub('_', item_data.description.strip()),
                "_".join([item_data.rootcd, item_data.rootid]),
                item_data.facility
            )

        response = requests.post(
            url=item_metadata['uri'],
            auth=auth,
            headers={
                "accept": "application/json;odata=verbose",
                "content-type": "application/json;odata=verbose",
                "X-HTTP-Method": "MERGE",
                "If-Match": item_metadata['etag'],
                "X-RequestDigest": xrd
            },
            data=data)
        print('Update:  %s' % response.status_code)
        if '20' not in str(response.status_code):
            print(data)
            pprint.pprint(response.json())
            print('Retrying connection')
            update_file_item(auth, xrd, item_metadata, item_data)
    except Exception, e:
        print(response.json())
        raise e


def get_test_files():

    with open('this_thing.txt', 'rb') as file:
        data = file.read()

    return [AttMetadata(
        fileid=u'123',
        modulecd=u'AI',
        keyid=u'321',
        rootcd=u'IN',
        rootid=u'567',
        filename=u'<file>G:\\this\\that\\thing\\derr',
        description=u'this is a thing, yo.',
        data=data,
        dataisstoredyn=u'N',
        is_restricted=u'N',
        facility=u'1234 A Facility'
    )]


def process_attachments(auth, file):
    """
    Insert attachment files and metadata into a Sharepoint site."""
    library = 'Action_Request_Test'
    bad_filename_match = re.compile(
        "[\&\'\"\/\\\:\<\>\*\[\]\;\?\|\#\~\$\=\{\}]")
    double_dot_match = re.compile("\.{2,}")
    try:
        xrd = get_x_request_digest(auth)
        filename = bad_filename_match.sub('_', file.filename)
        filename = double_dot_match.sub('.', filename)
        if file.dataisstoredyn == 'Y':
            file_data = file.data
        else:
            filename = ".".join([filename, 'txt'])
            file_data = file.filename.encode('utf-8')
        filename = filename[-110:] if len(filename) > 110 else filename
        filename = "_".join([file.rootcd, file.rootid, filename])
        print(filename, file.description)
        item_fields_uri = upload_binary(
            auth, xrd, library, filename, file_data)
        item_metadata = get_item_metadata(auth, item_fields_uri)
        update_file_item(auth, xrd, item_metadata, file)
    except Exception, e:
        raise e


def main():

    auth = get_auth()
    try:
        for file in get_attachments(get_non_ai_attachments_query()):
            process_attachments(auth, file)
    except:
        traceback.print_exc(file=sys.stdout)


if __name__ == '__main__':

    main()
