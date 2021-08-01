function Get-IndexSizeEstimate
{
<#

.SYNOPSIS
Returns estimated index size for a given column in a table

.DESCRIPTION
Calculates leaf-level index size for a given column in a table and returns the calculation and value in MB

.PARAMETER Instance
Specifies the SQL instance name

.PARAMETER Database
Specifies the database name

.PARAMETER Schema
Specifies the schema name

.PARAMETER Table
Specifies the table name

.PARAMETER Column
Specifies the column name(s) 

.PARAMETER Rows
Specifies the number of rows to calculate for instead of querying the table for COUNT()

.PARAMETER Fillfactor
Specifies the fillfactor (1-100) to use (default 100)

.OUTPUTS
Index leaf-level estimated size

.EXAMPLE
PS> Get-IndexSizeEstimate -Instance SQL2017-VM01 -Database TestDB -Schema dbo -Table IndexTesting -Column IndexCol1,VarIndexCol2
Using retrieved rowcount of 100000
Using variable-length column max length of 4 bytes
Using unique clustering key of total 4 bytes
Clustering key contains no null column(s)
0 clustering key column(s) are variable-length
Index contains 2 leaf columns total
1 total columns are variable-length
Using null bitmap of 3 bytes
Using variable-length column overhead size of 4 bytes
Index row size is 4 + 4 + 3 (null bitmap) + 4 (variable) + 1 (header) = 16 bytes
Leaf rows per page is 449
Leaf pages required is 223
Estimated nonclustered index size: 2 MB

#>
param
(
    [Parameter(position=0, mandatory=$true)][string]$Instance,
    [Parameter(position=1, mandatory=$true)][string]$Database,
    [Parameter(position=2, mandatory=$true)][string]$Schema,
    [Parameter(position=3, mandatory=$true)][string]$Table,
    [Parameter(position=4, mandatory=$true)][string[]]$Column,
    [Parameter(position=5, mandatory=$false)][long]$Rows,
    [Parameter(position=6, mandatory=$false)][int]$Fillfactor
)



Import-Module SqlServer

###### Pre-checks ######

# Parameter checks

if($Rows -and $Rows -lt 1)
{
    Write-Output "Rows parameter must be at least 1"

    return
}

if($Fillfactor -and ($Fillfactor -le 0 -or $Fillfactor -gt 100))
{
    Write-Output "Fillfactor must be an integer between 1 and 100"

    return;
}

# Check database exists and is online

$query = "SELECT 1
            FROM sys.databases
            WHERE [name] = '$($Database)'
            AND state_desc = 'ONLINE'"

try
{
    $check = Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $query -ConnectionTimeout 3 -QueryTimeout 3 -ErrorAction Stop

    if(!$check)
    {
        Write-Output "Error - database does not exist or is offline."

        return
    }
}
catch
{
    Write-Output "Error - can't connect to instance."
    Write-Output $error[0]

    return
}


# Check schema/table/column(s) exists
foreach($col in $Column)
{
    $query = "SELECT 1
                FROM sys.tables t
                INNER JOIN sys.schemas s
	                ON t.[schema_id] = s.[schema_id]
                INNER JOIN sys.columns c
	                ON t.[object_id] = c.[object_id]
                INNER JOIN sys.types ty
	                ON c.user_type_id = ty.user_type_id
                WHERE s.[name] = '$($Schema)'
                AND t.[name] = '$($Table)'
                AND c.[name] = '$($col)'"

    try
    {
        $check = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ConnectionTimeout 10 -QueryTimeout 300 -ErrorAction Stop

        if(!$check)
        {
            Write-Output "Error - schema/table/column doesn't exist for $($col)"
        
            return;
        }
    }
    catch
    {
        Write-Output "Error connecting to instance."
        Write-Output $error[0]

        return
    }
}

# Get SQL version

$query = "SELECT SUBSTRING(CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')), 1, CHARINDEX('.', CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')), 1) - 1) AS 'Version'"

try
{
    [int]$sqlversion = (Invoke-Sqlcmd -ServerInstance $Instance -Database master -Query $query -ConnectionTimeout 5 -QueryTimeout 5 -ErrorAction Stop).Version


}
catch
{
    Write-Output "Error retrieving SQL version"
    Write-Output $error[0]

    return
}

 
###### Code body ######

# Variable decarations
$leafcolumns = 0
$keylength = 0
$nullcolumns = $false
$nullbitmap = 0
$variablecolumns = 0
$variableoverheadsize = 0
$variablecolumnlist = 'text', 'ntext', 'image', 'varbinary', 'varchar', 'nvarchar'


# Get rowcount of table if not passed as parameter

if($Rows)
{
    $rowcount = $Rows

    Write-Output "Using passed rowcount of $($rowcount)"
}
else
{
    $query = "SELECT COUNT_BIG(*) AS 'Rowcount'
                FROM $($Schema).$($Table)"

    try
    {
        $rowcount = (Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ConnectionTimeout 10 -QueryTimeout 300 -ErrorAction Stop).Rowcount

        if($rowcount -eq 0)
        {
            Write-Output "Table is empty. Aborting function"

            return
        }

        Write-Output "Using retrieved rowcount of $($rowcount)"

    }
    catch
    {
        Write-Output "Error retrieving rowcount."
        Write-Output $error[0]

        return
    }
}

# Get key column(s) length/nullability and increment leaf and variable key counters

foreach($col in $Column)
{
    $query = "SELECT c.is_nullable, c.max_length AS 'MaxLength', ty2.[Name]
                FROM sys.tables t
                INNER JOIN sys.schemas s
	                ON t.[schema_id] = s.[schema_id]
                INNER JOIN sys.columns c
	                ON t.[object_id] = c.[object_id]
                INNER JOIN sys.types ty
	                ON c.user_type_id = ty.user_type_id
                INNER JOIN sys.types ty2
	                ON ty.system_type_id = ty2.user_type_id
                WHERE s.[name] = '$($Schema)'
                AND t.[name] = '$($Table)'
                AND c.[name] = '$($col)'"

    try
    {
        $result = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ConnectionTimeout 5 -QueryTimeout 5 -ErrorAction Stop

        $keylength += $result.MaxLength

        $leafcolumns++

        # Set null columns flag true if index column is nullable (used for SQL < 2012)
        if($result.is_nullable)
        {
            $nullcolumns = $true
        }

        if($variablecolumnlist -contains $result.Name)
        {
            $variablecolumns++

            Write-Output "$($col): Using variable-length column max length of $($result.MaxLength) bytes"
        }
        else
        {
            Write-Output "$($col): Using fixed-length column max length of $($result.MaxLength) bytes"
        }
    
    }
    catch
    {
        Write-Output "Error retrieving column max length"
        Write-Output $error[0]
    
        return
    }
}


# Get clustered index size, nullability, column count and uniqueness (if exists)
$query = "SELECT i.is_unique, MAX(CAST(is_nullable AS TINYINT)) AS 'NullColumns', COUNT(*) AS 'NumKeyCols',  SUM(c.max_length) AS 'SummedMaxLength'
            FROM sys.tables t
            INNER JOIN sys.schemas s
	            ON t.[schema_id] = s.[schema_id]
            INNER JOIN sys.indexes i
	            ON t.[object_id] = i.[object_id]
            INNER JOIN sys.index_columns ic
	            ON i.[object_id] = ic.[object_id]
	            AND i.index_id = ic.index_id
            INNER JOIN sys.columns c
	            ON ic.[object_id] = c.[object_id]
	            AND ic.column_id = c.column_id
            WHERE s.[name] = '$($Schema)'
            AND t.[name] = '$($Table)'
            AND i.[type] = 1
            GROUP BY i.is_unique"

try
{
    $clusteringkey = Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ConnectionTimeout 5 -QueryTimeout 5 -ErrorAction Stop

    # If there's no clustering key
    if(!$clusteringkey)
    {
        # Use value for heap RID (8 bytes)
        $rowlocatorlength = 8
        Write-Output "Table is a heap, using RID of 8 bytes"
    }
    else
    {
        # Increment leaf column count by clustering key count
        $leafcolumns += $clusteringkey.NumKeyCols

        if($clusteringkey.is_unique -eq 1)
        {
            $rowlocatorlength = $clusteringkey.SummedMaxLength

            Write-Output "Using unique clustering key of total $($rowlocatorlength) bytes"
        }
        else
        {
            # Need to add 4 bytes for uniquifier, and also increment $variablecolumns
            $rowlocatorlength = $clusteringkey.SummedMaxLength + 4
            $variablecolumns++

            Write-Output "Using nonunique clustering key of total $($rowlocatorlength) bytes with uniquifier"
        }

        # Check if any null columns exist in clustering key and set flag if so
        if($clusteringkey.NullColumns -eq 1)
        {
            $nullcolumns = $true

            Write-Output "Clustering key contains null column(s)"
        }
        else
        {
            Write-Output "Clustering key contains no null column(s)"
        }

        # Get count of clustering key variable colunns
        $query = "SELECT COUNT(*) AS 'Count'
                    FROM sys.tables t
                    INNER JOIN sys.schemas s
	                    ON t.[schema_id] = s.[schema_id]
                    INNER JOIN sys.indexes i
	                    ON t.[object_id] = i.[object_id]
                    INNER JOIN sys.index_columns ic
	                    ON i.[object_id] = ic.[object_id]
	                    AND i.index_id = ic.index_id
                    INNER JOIN sys.columns c
	                    ON ic.[object_id] = c.[object_id]
	                    AND ic.column_id = c.column_id
                    INNER JOIN sys.types ty
	                    ON c.user_type_id = ty.user_type_id
                    INNER JOIN sys.types ty2
	                    ON ty.system_type_id = ty2.user_type_id
                    WHERE s.[name] = '$($schema)'
                    AND t.[name] = '$($Table)'
                    AND i.[type] = 1
                    AND ty2.[name] IN ('text', 'ntext', 'image', 'varbinary', 'varchar', 'nvarchar')"

        $variableclusteringcolumns = (Invoke-Sqlcmd -ServerInstance $Instance -Database $Database -Query $query -ConnectionTimeout 5 -QueryTimeout 5 -ErrorAction Stop).Count

        Write-Output "$($variableclusteringcolumns) clustering key column(s) are variable-length"

        $variablecolumns += $variableclusteringcolumns

    }
}
catch
{
    Write-Output "Error retrieving clustering key info"
    Write-Output $error[0]

    return
}


Write-Output "Index contains $($leafcolumns) leaf columns total"


if(($nullcolumns) -and ($sqlversion -lt 11))
{
    Write-Output "Leaf record contains null columns"
}

Write-Output "$($variablecolumns) total columns are variable-length"
    
# Calculate null bitmap size
# If version is >= 2012 or null columns exist, add null bitmap - else don't add
if(($sqlversion -ge 11) -or ($nullcolumns -eq $true))
{
    $nullbitmap = [Math]::Floor(2 + (($leafcolumns + 7) / 8))

    Write-Output "Using null bitmap of $($nullbitmap) bytes"
}

# Calculate variable-length overhead size
if($variablecolumns -gt 0)
{
    $variableoverheadsize = 2 + ($variablecolumns * 2) 

    Write-Output "Using variable-length column overhead size of $($variableoverheadsize) bytes"
}

# Calculate total index size
$indexsize = $keylength + $rowlocatorlength + $nullbitmap + $variableoverheadsize + 1

Write-Output "Index row size is $($keylength) + $($rowlocatorlength) (locator) + $($nullbitmap) (null bitmap) + $($variableoverheadsize) (variable) + 1 (header) = $($indexsize) bytes"

# Calculate leaf rows per page - adding 2 bytes to indexsize for slot array entry 
$leafrowsperpage = [Math]::Floor(8096 / ($indexsize + 2))

Write-Output "Leaf rows per page is $leafrowsperpage"

# Use full pages if not specifying fillfactor
if(!$Fillfactor)
{
    $leafpages = [Math]::Ceiling($rowcount / $leafrowsperpage)

    Write-Output "Leaf pages required is $($leafpages)"
}
else
{
    Write-Output "Using fillfactor of $($Fillfactor)"

    $freerowsperpage = [Math]::Floor(8096 * ((100 - $Fillfactor) / 100) / ($indexsize + 2))

    $leafpages = [Math]::Ceiling($rowcount / ($leafrowsperpage - $freerowsperpage))

    Write-Output "Leaf pages required is $($leafpages)"
}


###### Final result ######

$estimate = $leafpages * 8192

$estimateMB = [Math]::Round($estimate / 1024 / 1024)

Write-Output "Estimated nonclustered index size: $($estimateMB) MB"

}


