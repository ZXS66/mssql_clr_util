using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Server;

// reference: https://en.dirceuresende.com/blog/sql-server-how-to-concatenate-rows-by-grouping-data-by-a-grouped-concatenation-column/
// https://learn.microsoft.com/en-us/sql/relational-databases/clr-integration-database-objects-user-defined-functions/clr-user-defined-aggregate-invoking-functions

/// <summary>
/// Concatenating columns into grouped strings consists of transforming rows into a concatenated string, already present in other DBMSs such as MySQL (GROUP_CONCAT), Oracle (XMLAGG) and PostgreeSQL (STRING_AGG or ARRAY_TO_STRING(ARRAY_AGG()))
/// </summary>
[Serializable]
[SqlUserDefinedAggregate(
    Format.UserDefined,
    IsInvariantToNulls = false,
    IsInvariantToDuplicates = false,
    IsInvariantToOrder = true,
    MaxByteSize = -1)]
public struct group_concat : IBinarySerialize, INullable
{
    private StringBuilder _accumulator;
    private string _delimiter;

    public bool IsNull { get; private set; }

    public void Init()
    {
        _accumulator = new StringBuilder();
        _delimiter = string.Empty;
        this.IsNull = true;
    }

    public void Accumulate([SqlFacet(MaxSize = -1)] SqlString Value, [SqlFacet(MaxSize = -1)] SqlString Delimiter)
    {

        if (Value.IsNull) return;

        if (!Delimiter.IsNull & Delimiter.Value.Length > 0)
        {
            _delimiter = Delimiter.Value;
            if (_accumulator.Length > 0)
                _accumulator.Append(Delimiter.Value);

        }

        _accumulator.Append(Value.Value);
        IsNull = false;
    }

    public void Merge(group_concat group)
    {
        if (_accumulator.Length > 0
            & group._accumulator.Length > 0) _accumulator.Append(_delimiter);

        _accumulator.Append(group._accumulator.ToString());
    }

    public SqlString Terminate()
    {
        return new SqlString(_accumulator.ToString());
    }

    void IBinarySerialize.Read(System.IO.BinaryReader r)
    {
        _delimiter = r.ReadString();
        _accumulator = new StringBuilder(r.ReadString());

        if (_accumulator.Length != 0) this.IsNull = false;
    }

    void IBinarySerialize.Write(System.IO.BinaryWriter w)
    {
        w.Write(_delimiter);
        w.Write(_accumulator.ToString());
    }
}
