#!/usr/bin/env python3
# *****************************************************************************
# *                                                                           *
# *                         a u d i t _ i n g e s t                           *
# *                                                                           *
# *****************************************************************************

# The following envars control execution
#
# "INGEST_BUTLER"
#       Path to the Butler repository that should be used.

# "AUDIT_INGEST_DEBUG"
#       If this envar is set, even without a value, debugging is enabled.

# "AUDIT_INGEST_MAXITEMS"
#       An integer number that specifies the maximum number of bulk requests
#       that can be handed to Rucio. The default is 1000, the likely limit.

# "AUDIT_INGEST_MAXUUIDS"
#       An integer number that specifies the maximum number of uuid requests
#       that can be handed to Butler at one time. The default is 1000.

import argparse
import errno
import json
import os
import sys
import uuid

from rucio.client.didclient import DIDClient

from lsst.daf.butler import Butler

# Rucio changed the naming conventions for the metadata client with no
# indication of which release it occured in. So, we do if/then/else load.
# try:
#    from rucio.client.metaclient import MetaClient as MetaConventionClient
# except Exception as e:  # noqa: F841

# from rucio.client.metaconventionsclient import MetaConventionClient

# The following will be initialized in init_globals)
#
butler = None
cmd_info = None
Debug = False
DIDclient = None
MAX_ITEMS = 0
MAX_UUIDS = 0


# *****************************************************************************
# *                                 E m s g                                   *
# *****************************************************************************

# Print to stderr a message
#
def emsg(rc, txt, *, verbose=False):

    if not verbose or cmd_info.verbose:
        print('audit_ingest:', txt, file=sys.stderr)
    if (rc < 0):
        rc = -rc
    if rc:
        sys.exit(rc)


# *****************************************************************************
# *                             f u l l _ l f n                               *
# *****************************************************************************

def full_lfn(lfm_vec, lfn_num):
    return lfm_vec[lfn_num][0] + ':' + lfm_vec[lfn_num][1]


# *****************************************************************************
# *                         g e t _ M a x I t e m s                           *
# *****************************************************************************

def get_maxitems(e_var, e_dflt):

    # Get the maximum allowed items in a bulk query
    #
    max_num = e_dflt
    x = os.getenv(e_var, None)
    if x is not None:
        try:
            max_num = int(x)
            if max_num < 1:
                max_num = 1
        except Exception as e:
            emsg(0, "{}={} is not an integer ({})".format(e_var, x, e))

    if Debug:
        emsg(0, '{} = {}'.format(e_var, max_num))

    return max_num


# *****************************************************************************
# *                             G e t _ l f n s                               *
# *****************************************************************************

def get_lfns(scope, dsn):

    if Debug:
        emsg(0, 'list_lfns({}, {})'.format(scope, dsn))

    try:
        did_vec = DIDclient.list_files(scope, dsn)
    except Exception as e:
        emsg(8, "Unable to get lfns from {}:{}; {}".format(scope, dsn, e))

    # Convert an object list to a standard list
    #
    lfn_vec = []
    for file in did_vec:
        lfn_vec.append([file['scope'], file['name']])

    # Return the vector
    #
    return lfn_vec


# *****************************************************************************
# *                            G e t _ u u i d s                              *
# *****************************************************************************

def get_uuids(dsn, lfn_vec):

    if Debug:
        emsg(0, 'get_uuids({}, lfn_vec[{}])'.format(dsn, len(lfn_vec)))

    # Conevert the lfn_vec from [scope, name] to a vector of dicts
    did_vec = []
    for did in lfn_vec:
        did_vec.append({'scope': did[0], 'name': did[1]})

    # Get the metadata for all of the file did's
    #
    lfn_skp = 0
    lfn_num = 0
    lfn_err = 0
    meta_vec = []
    uuid_vec = []
    while (len(did_vec) > 0):
        try:
            m_len = min(len(did_vec), MAX_ITEMS)
            m_vec = DIDclient.get_metadata_bulk(did_vec[0:m_len],
                                                inherit=True, plugin='JSON')
        except Exception as e:
            emsg(8, 'Unable to get bulk metadata for files in {}; {}'
                    .format(dsn, e))

        # Remove the queried elements from the list
        #
        del did_vec[:m_len]

        # Extract the uuid from the metadata for each filep
        #
        for meta in m_vec:
            if Debug:
                emsg(0, 'FILE >>> {}'.format(lfn_vec[lfn_num][1]))
                emsg(0, 'META >>> {}'.format(meta))

            if ('rubin_butler' not in meta.keys()
               or 'rubin_sidecar' not in meta.keys()):
                emsg(0, ("Skipping uningestible file {}; "
                     + "missing butler metadata!")
                     .format(full_lfn(lfn_vec, lfn_num)), verbose=True)
                lfn_skp += 1
                meta_vec.append(None)
                uuid_vec.append(None)
            else:
                try:
                    uuid = json.loads(meta['rubin_sidecar'])['id']
                    meta_vec.append(meta['rubin_sidecar'])
                    uuid_vec.append(uuid)
                except Exception as e:
                    emsg(0, "Invalid JSON or missing uuid for file {}; {}!"
                            .format(full_lfn(lfn_vec, lfn_num), e),
                            verbose=True)
                    meta_vec.append(None)
                    uuid_vec.append(None)
                    lfn_err += 1
            lfn_num += 1

    if Debug:
        emsg(0, "Extracted {} uuids from {} files; {} errors and {} skips."
                .format(lfn_num - (lfn_err + lfn_skp), lfn_num, lfn_err,
                        lfn_skp))

    return uuid_vec, meta_vec


# *****************************************************************************
# *                         R e p o r t _ A u d i t                           *
# *****************************************************************************

def report_audit(txt, lfn_vec, sel_vec, list_type, meta_vec=None):

    want = list_type in cmd_info.list
    if want or cmd_info.verbose:
        print("-----", len(sel_vec), txt, file=sys.stderr)
        if want:
            if cmd_info.prefix:
                pfx = '!{}: '.format(list_type)
            else:
                pfx = ''
            for i in sel_vec:
                print(pfx, full_lfn(lfn_vec, i), sep='')
                if meta_vec is not None and cmd_info.meta:
                    print(meta_vec[i])


# *****************************************************************************
# *                         P r o c e s s _ r e f s                           *
# *****************************************************************************

def process_refs(uuid_qry, uuid_set):

    # Get the refs for this list of uuid objects, the function thrown nothing
    #
    ref_vec = butler.get_many_datasets(uuid_qry)

    # For each uuid in the returned refs, remove it from the lfn mappping dict
    #
    nerrs = 0
    for ref in ref_vec:
        try:
            uuid_set.remove(str(ref.id))
        except Exception as e:  # noqa: F841
            emsg(0, "Butler returned an unqueried uuid: {}".format(ref.id),
                 verbose=True)
            nerrs += 1
    return nerrs


# *****************************************************************************
# *                          A u d i t _ R u c i o                            *
# *****************************************************************************

def audit_rucio(dsn):

    # Make sure we have a proper dataset DID
    #
    sd = dsn.split(':', 1)
    if len(sd) != 2:
        emsg(errno.EINVAL, "Invalid rucio dataset DID: {}".format(dsn))
    scope = sd[0]
    dsdid = sd[1]

    # Get the list of logical file names
    #
    lfn_vec = get_lfns(scope, dsdid)

    # Get the associated butler uuid
    #
    uid_vec, meta_vec = get_uuids(dsn, lfn_vec)

    # Verify that these uuid exist in butler. During the process we collect
    # The DID that we could not check, if any.
    #
    f_num = 0
    f_nope = []    # files not check because not ingestable via Rucio
    f_miss = []    # files that are ingestable but not found in butler
    f_dups = []    # Files not checked because it has a duplicate uuid
    b_errs = 0     # Number of unknown uuids returned by Butler
    uuid2fn = {}   # Map from uuid to lfn index
    uuid_qry = []  # Argument to get_many_files()
    uuid_set = set()

    # Process all of he uuid that were returned
    #
    for uuid_str in uid_vec:
        if not uuid_str:
            f_nope.append(f_num)
        elif uuid_str in uuid2fn:
            emsg(0, "Rucio returned the same uuid '{}' for DIDs '{}' and '{}'!"
                    .format(uuid_str, full_lfn(lfn_vec, uuid2fn[uuid_str]),
                            full_lfn(lfn_vec, f_num)), verbose=True)
            f_dups.append(f_num)
        else:
            uuid_qry.append(uuid.UUID(uuid_str))
            uuid2fn[uuid_str] = f_num
            uuid_set.add(uuid_str)

        # If we now have the maximum number of uuid we should query, do so now
        #
        if len(uuid_qry) >= MAX_UUIDS:
            b_errs += process_refs(uuid_qry, uuid_set)
            uuid_qry.clear()

        f_num += 1

    # Process any leftover uuids
    #
    if uuid_qry:
        b_errs += process_refs(uuid_qry, uuid_set)

    # At this point anything left in uuid_set is missing in butler
    #
    if uuid_set:
        for uuid_str in uuid_set:
            f_miss.append(uuid2fn[uuid_str])

    # Provide final report (we may want this to be selectable)
    #
    print("Audit results for files in Rucio dataset {}:".format(dsn),
          file=sys.stderr)
    if len(f_nope) > 0:
        report_audit("noningestable files due to missing metadata.",
                     lfn_vec, f_nope, 'nometa')
    if len(f_dups) > 0:
        report_audit("ingestable files registered Rucio but not checked "
                     + "due to duplicate uuid.", lfn_vec, f_dups, 'uuiderrs')
    if len(f_miss) > 0:
        report_audit("ingestable files registered Rucio but not in butler.",
                     lfn_vec, f_miss, 'missing', meta_vec)

    # We issue this warning as it will always result in some files appearing
    # to be registered in Rucio but not in Butler.
    #
    if b_errs and cmd_info.verbose:
        print("----- Warning: Butler returned {} incorrect uuids!"
              .format(b_errs), file=sys.stderr)

    no_go = len(f_nope) + len(f_dups) + len(f_miss)
    if not no_go:
        print("+++++ All {} files registered in Rucio are also in Butler!"
              .format(len(lfn_vec)), file=sys.stderr)
        return 0

    in_butler = len(lfn_vec) - no_go
    print("---- {} out of {} files registered in Rucio are in Butler!"
          .format(in_butler, len(lfn_vec)), file=sys.stderr)
    return 13


# *****************************************************************************
# *                               s y n t a x                                 *
# *****************************************************************************

def syntax():

    opt = argparse.ArgumentParser(add_help=True, allow_abbrev=False,
                                  usage=("audit_ingest [options] rucio "
                                         "<scope>:<did>")
                                  )

    opt.add_argument('-d', '--debug', action='store_true', default=False,
                     help='Turns on debugging output.'
                     )

    opt.add_argument('-l', '--list', nargs='+',
                     choices=["missing", "nometa", "uuiderr"],
                     default=["missing"],
                     help='List files with specified issues.'
                     )

    opt.add_argument('-m', '--meta', action='store_true', default=False,
                     help='Print butler sidecar metadata for uningested files.'
                     )

    opt.add_argument('-p', '--prefix', action='store_true', default=False,
                     help='Add prefix identifier in file listing.'
                     )

    opt.add_argument('-t', '--terse', action='store_false',
                     dest='verbose', default=False,
                     help='Suppresses error message details, the default.'
                     )

    opt.add_argument('-v', '--verbose', action='store_true',
                     dest='verbose',
                     help='Prints the details of encountered problems.'
                     )

    opt.add_argument('repository', choices=['rucio'],
                     help='Specifies the repository to be audited.'
                     )

    opt.add_argument('target',
                     help='The collection or dataset in the repository.'
                     )

    return opt


# *****************************************************************************
# *                         i n i t _ g l o b a l s                           *
# *****************************************************************************

def init_globals():

    # First step is to do a parse of the command line
    #
    global cmd_info
    sntx = syntax()
    cmd_info = sntx.parse_args()

    # Check for debugging
    #
    global Debug
    if cmd_info.debug:
        Debug = True
    else:
        x = os.getenv("AUDIT_INGEST_DEBUG", None)
        if x is not None:
            Debug = True

    # Initialize the did client
    #
    global DIDclient
    try:
        DIDclient = DIDClient()
    except Exception as e:
        emsg(8, "Error creating DID client: {}".format(e))

    # Initialize the correct butler repo
    #
    b_repo = os.getenv("INGEST_BUTLER", None)
    if b_repo is None:
        raise ValueError("Please point unix envorinment INGEST_BUTLER to a Butler")
    global butler
    try:
        butler = Butler(b_repo)
    except Exception as e:
        emsg(8, "Error creating Butler client: {}".format(e))

    # Initialize rucio  MAX_ITEMS
    #
    global MAX_ITEMS
    MAX_ITEMS = get_maxitems("AUDIT_INGEST_MAXITEMS", 1000)

    # Initialize butler MAX_UUIDS
    #
    global MAX_UUIDS
    MAX_UUIDS = get_maxitems("AUDIT_INGEST_MAXUUIDS", 1000)


# *****************************************************************************
# *                                 M a i n                                   *
# *****************************************************************************

# The actual guts of the script
#
def main():

    # Check if data registry is supported and get audit target
    #
    if cmd_info.repository == "rucio":
        return audit_rucio(cmd_info.target)

    # We do not support the registry for audit
    #
    emsg(errno.ENOTSUP, 'Auditing "{}" registry not supported!'
         .format(cmd_info.repository))


if __name__ == "__main__":

    # Setup globals
    #
    init_globals()

    # Continue
    #
    sys.exit(main())
