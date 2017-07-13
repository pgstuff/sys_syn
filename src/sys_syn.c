/*
Copyright (c) 2017, PostgreSQL Global Development Group
*/

#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
//#include "funcapi.h"
//#include "libpq/pqformat.h"
#include "miscadmin.h"
//#include "utils/builtins.h"
//#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "access/hash.h"
#include "utils/pg_crc.h"
#include "port/pg_crc32c.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "utils/formatting.h"

PG_MODULE_MAGIC;

TransactionId   cache_topxid    = InvalidTransactionId;
Oid             tinyint_typoid  = InvalidOid;
Oid             citext_typoid   = InvalidOid;

void cache_lookups(void);

void cache_lookups()
{
        TransactionId topxid = GetTopTransactionIdIfAny();

        if (cache_topxid != InvalidTransactionId && cache_topxid == topxid)
                return;

        cache_topxid = topxid;

        tinyint_typoid  = TypenameGetTypid("tinyint");
        citext_typoid   = TypenameGetTypid("citext");
        // TODO:  Add varint, telephone
}

PG_FUNCTION_INFO_V1(hash_id);
Datum
hash_id(PG_FUNCTION_ARGS)
{
        HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
        Oid             tupType;
        int32           tupTypmod;
        TupleDesc       tupdesc;
        HeapTupleData   tuple;
        int             ncolumns;
        int             i;
        Datum           *values;
        bool            *nulls;
        uint32          hash = 0;
        int64           val;
        uint32          lohalf;
        uint32          hihalf;

        cache_lookups();
        check_stack_depth();            /* recurses for record-type columns */

        /* Extract type info from the tuple itself */
        tupType         = HeapTupleHeaderGetTypeId(rec);
        tupTypmod       = HeapTupleHeaderGetTypMod(rec);
        tupdesc         = lookup_rowtype_tupdesc(tupType, tupTypmod);
        ncolumns        = tupdesc->natts;

        /* Build a temporary HeapTuple control structure */
        tuple.t_len     = HeapTupleHeaderGetDatumLength(rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid= InvalidOid;
        tuple.t_data    = rec;

        values = (Datum *) palloc(ncolumns * sizeof(Datum));
        nulls = (bool *) palloc(ncolumns * sizeof(bool));

        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);

        for (i = 0; i < ncolumns; i++)
        {
                Oid column_type = tupdesc->attrs[i]->atttypid;

                /* Ignore dropped columns in datatype */
                if (tupdesc->attrs[i]->attisdropped)
                        continue;

                if (nulls[i])
                        continue;

                if (tupdesc->attrs[i]->attlen == -1)
                {
                        //Size            len;
                        struct varlena *argval;

                        argval = PG_DETOAST_DATUM_PACKED(values[i]);

                        if (column_type == citext_typoid)
                        {
                                char       *lower_str;

                                lower_str = str_tolower(VARDATA_ANY(argval), VARSIZE_ANY_EXHDR(argval), DEFAULT_COLLATION_OID);
                                hash ^= DatumGetUInt32(hash_any((unsigned char *) lower_str, strlen(lower_str)));
                        }
                        else
                        {
                                //len = toast_raw_datum_size(values[i]);
                                hash ^= DatumGetUInt32(hash_any((unsigned char *) VARDATA_ANY(argval),
                                        VARSIZE_ANY_EXHDR(argval)));

                                //argval = PG_DETOAST_DATUM(values[i]);
                                //hash ^= DatumGetUInt32(hash_any((unsigned char *) VARDATA(argval),
                                //        VARSIZE(argval) - VARHDRSZ));
                        }
                }
                else if (tupdesc->attrs[i]->attbyval)
                {
                        switch (tupdesc->attrs[i]->attlen)
                        {
                                case 1:
                                        lohalf = GET_1_BYTE(values[i]);

                                        if (column_type == tinyint_typoid)
                                        {
                                                if ((int8) lohalf < 0)
                                                {
                                                        hash ^= hash_uint32(0xFFFFFFFF);
                                                        lohalf = lohalf | 0xFFFFFF00;
                                                }
                                                else
                                                {
                                                        hash ^= hash_uint32(0);
                                                }
                                        }

                                        hash ^= DatumGetUInt32(hash_uint32(lohalf));
                                        break;
                                case 2:
                                        lohalf = GET_2_BYTES(values[i]);

                                        if (column_type == INT2OID)
                                        {
                                                if ((int16) lohalf < 0)
                                                {
                                                        hash ^= hash_uint32(0xFFFFFFFF);
                                                        lohalf = lohalf | 0xFFFF0000;
                                                }
                                                else
                                                {
                                                        hash ^= hash_uint32(0);
                                                }
                                        }

                                        hash ^= DatumGetUInt32(hash_uint32(lohalf));
                                        break;
                                case 4:
                                        lohalf = GET_4_BYTES(values[i]);

                                        if (column_type == INT4OID)
                                        {
                                                if ((int32) lohalf < 0)
                                                {
                                                        hash ^= hash_uint32(0xFFFFFFFF);
                                                }
                                                else
                                                {
                                                        hash ^= hash_uint32(0);
                                                }
                                        }

                                        hash ^= DatumGetUInt32(hash_uint32(GET_4_BYTES(values[i])));
                                        break;
#if SIZEOF_DATUM == 8
                                case 8:
                                        val = GET_8_BYTES(values[i]);
                                        lohalf = (uint32) val;
                                        hihalf = (uint32) (val >> 32);
                                        lohalf ^= (val >= 0) ? hihalf : ~hihalf;
                                        hash ^= DatumGetUInt32(hash_uint32(hihalf));
                                        hash ^= DatumGetUInt32(hash_uint32(lohalf));
                                        break;
#endif
                                default:
                                        Assert(false);  /* cannot happen */
                        }
                }
                else
                {
                        hash ^= DatumGetUInt32(hash_any((unsigned char *) DatumGetPointer(values[i]),
                                tupdesc->attrs[i]->attlen));
                }
        }

        pfree(values);
        pfree(nulls);
        ReleaseTupleDesc(tupdesc);

        PG_RETURN_INT32((int32) hash);
}

PG_FUNCTION_INFO_V1(crc32_id);
Datum
crc32_id(PG_FUNCTION_ARGS)
{
        HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
        Oid             tupType;
        int32           tupTypmod;
        TupleDesc       tupdesc;
        HeapTupleData   tuple;
        int             ncolumns;
        int             i;
        Datum           *values;
        bool            *nulls;
        pg_crc32        valcrc;
        int64           val64;

        cache_lookups();
        check_stack_depth();            /* recurses for record-type columns */

        /* Extract type info from the tuple itself */
        tupType         = HeapTupleHeaderGetTypeId(rec);
        tupTypmod       = HeapTupleHeaderGetTypMod(rec);
        tupdesc         = lookup_rowtype_tupdesc(tupType, tupTypmod);
        ncolumns        = tupdesc->natts;

        /* Build a temporary HeapTuple control structure */
        tuple.t_len     = HeapTupleHeaderGetDatumLength(rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid= InvalidOid;
        tuple.t_data    = rec;

        values = (Datum *) palloc(ncolumns * sizeof(Datum));
        nulls = (bool *) palloc(ncolumns * sizeof(bool));

        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);

        INIT_TRADITIONAL_CRC32(valcrc);

        for (i = 0; i < ncolumns; i++)
        {
                Oid column_type = tupdesc->attrs[i]->atttypid;

                /* Ignore dropped columns in datatype */
                if (tupdesc->attrs[i]->attisdropped)
                        continue;

                if (nulls[i])
                        continue;

                if (tupdesc->attrs[i]->attlen == -1)
                {
                        //Size            len;
                        struct varlena *argval;

                        //len = toast_raw_datum_size(values[i]);
                        argval = PG_DETOAST_DATUM_PACKED(values[i]);

                        if (column_type == citext_typoid)
                        {
                                char       *lower_str;

                                lower_str = str_tolower(VARDATA_ANY(argval), VARSIZE_ANY_EXHDR(argval), DEFAULT_COLLATION_OID);
                                COMP_TRADITIONAL_CRC32(valcrc, lower_str, strlen(lower_str));
                        }
                        else
                        {
                                COMP_TRADITIONAL_CRC32(valcrc, VARDATA_ANY(argval), VARSIZE_ANY_EXHDR(argval));
                        }
                }
                else if (tupdesc->attrs[i]->attbyval)
                {
                        switch (tupdesc->attrs[i]->attlen)
                        {
                                case 1:
                                        val64 = (int64) GET_1_BYTE(values[i]);
                                        if (column_type == tinyint_typoid)
                                        {
                                                if ((int8) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFFFFFFFF00;

                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                val64 = (int64) GET_1_BYTE(values[i]);
                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 1);
                                        }
                                        break;
                                case 2:
                                        val64 = (int64) GET_2_BYTES(values[i]);
                                        if (column_type == INT2OID)
                                        {
                                                if ((int16) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFFFFFF0000;

                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 2);
                                        }
                                        break;
                                case 4:
                                        val64 = (int64) GET_4_BYTES(values[i]);

                                        if (column_type == INT4OID)
                                        {
                                                if ((int32) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFF00000000;

                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                COMP_TRADITIONAL_CRC32(valcrc, &val64, 4);
                                        }
                                        break;
#if SIZEOF_DATUM == 8
                                case 8:
                                        val64 = GET_8_BYTES(values[i]);
                                        COMP_TRADITIONAL_CRC32(valcrc, &val64, 8);
                                        break;
#endif
                                default:
                                        Assert(false);  /* cannot happen */
                        }
                }
                else
                {
                        COMP_TRADITIONAL_CRC32(valcrc, DatumGetPointer(values[i]), tupdesc->attrs[i]->attlen);
                }
        }

        FIN_TRADITIONAL_CRC32(valcrc);
        pfree(values);
        pfree(nulls);
        ReleaseTupleDesc(tupdesc);

        PG_RETURN_INT32((int32) valcrc);
}

PG_FUNCTION_INFO_V1(crc32c_id);
Datum
crc32c_id(PG_FUNCTION_ARGS)
{
        HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
        Oid             tupType;
        int32           tupTypmod;
        TupleDesc       tupdesc;
        HeapTupleData   tuple;
        int             ncolumns;
        int             i;
        Datum           *values;
        bool            *nulls;
        pg_crc32        valcrc;
        int64           val64;

        cache_lookups();
        check_stack_depth();            /* recurses for record-type columns */

        /* Extract type info from the tuple itself */
        tupType         = HeapTupleHeaderGetTypeId(rec);
        tupTypmod       = HeapTupleHeaderGetTypMod(rec);
        tupdesc         = lookup_rowtype_tupdesc(tupType, tupTypmod);
        ncolumns        = tupdesc->natts;

        /* Build a temporary HeapTuple control structure */
        tuple.t_len     = HeapTupleHeaderGetDatumLength(rec);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid= InvalidOid;
        tuple.t_data    = rec;

        values = (Datum *) palloc(ncolumns * sizeof(Datum));
        nulls = (bool *) palloc(ncolumns * sizeof(bool));

        /* Break down the tuple into fields */
        heap_deform_tuple(&tuple, tupdesc, values, nulls);

        INIT_CRC32C(valcrc);

        for (i = 0; i < ncolumns; i++)
        {
                Oid column_type = tupdesc->attrs[i]->atttypid;

                /* Ignore dropped columns in datatype */
                if (tupdesc->attrs[i]->attisdropped)
                        continue;

                if (nulls[i])
                        continue;

                if (tupdesc->attrs[i]->attlen == -1)
                {
                        //Size            len;
                        struct varlena *argval;

                        //len = toast_raw_datum_size(values[i]);
                        argval = PG_DETOAST_DATUM_PACKED(values[i]);

                        if (column_type == citext_typoid)
                        {
                                char       *lower_str;

                                lower_str = str_tolower(VARDATA_ANY(argval), VARSIZE_ANY_EXHDR(argval), DEFAULT_COLLATION_OID);
                                COMP_CRC32C(valcrc, lower_str, strlen(lower_str));
                        }
                        else
                        {
                                COMP_CRC32C(valcrc, VARDATA_ANY(argval), VARSIZE_ANY_EXHDR(argval));
                        }
                }
                else if (tupdesc->attrs[i]->attbyval)
                {
                        switch (tupdesc->attrs[i]->attlen)
                        {
                                case 1:
                                        val64 = (int64) GET_1_BYTE(values[i]);
                                        if (column_type == tinyint_typoid)
                                        {
                                                if ((int8) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFFFFFFFF00;

                                                COMP_CRC32C(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                val64 = (int64) GET_1_BYTE(values[i]);
                                                COMP_CRC32C(valcrc, &val64, 1);
                                        }
                                        break;
                                case 2:
                                        val64 = (int64) GET_2_BYTES(values[i]);
                                        if (column_type == INT2OID)
                                        {
                                                if ((int16) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFFFFFF0000;

                                                COMP_CRC32C(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                COMP_CRC32C(valcrc, &val64, 2);
                                        }
                                        break;
                                case 4:
                                        val64 = (int64) GET_4_BYTES(values[i]);

                                        if (column_type == INT4OID)
                                        {
                                                if ((int32) val64 < 0)
                                                        val64 = val64 | 0xFFFFFFFF00000000;

                                                COMP_CRC32C(valcrc, &val64, 8);
                                        }
                                        else
                                        {
                                                COMP_CRC32C(valcrc, &val64, 4);
                                        }
                                        break;
#if SIZEOF_DATUM == 8
                                case 8:
                                        val64 = GET_8_BYTES(values[i]);
                                        COMP_CRC32C(valcrc, &val64, 8);
                                        break;
#endif
                                default:
                                        Assert(false);  /* cannot happen */
                        }
                }
                else
                {
                        COMP_CRC32C(valcrc, DatumGetPointer(values[i]), tupdesc->attrs[i]->attlen);
                }
        }

        FIN_CRC32C(valcrc);
        pfree(values);
        pfree(nulls);
        ReleaseTupleDesc(tupdesc);

        PG_RETURN_INT32((int32) valcrc);
}
