Attribute VB_Name = "TaxiZoneCleaner"
' TaxiZoneCleaner - cleans and harmonizes the NYC TLC Taxi Zone Lookup table
' (taxi_zone_lookup.csv) locally in Excel, before it is loaded to BigQuery.
'
' Entry point: CleanTaxiZones
'   1. reads data\taxi_zone_lookup.csv (UTF-8), or asks for a file
'   2. trims/normalizes text, maps borough and service_zone aliases to one
'      canonical spelling, turns "N/A"/empty values into "Unknown",
'      rejects non-numeric or duplicate LocationIDs
'   3. writes two sheets: zones_clean (result) and zones_log (every change,
'      rejection and warning)
'   4. exports <input>_clean.csv (UTF-8, no BOM) next to the input file
'
' No library reference needed (late binding). Code is ASCII-only on purpose:
' the VBA editor imports .bas files as ANSI.
Option Explicit

Private Const UNKNOWN_VALUE As String = "Unknown"
Private Const DEFAULT_FILE As String = "taxi_zone_lookup.csv"
Private Const SHEET_CLEAN As String = "zones_clean"
Private Const SHEET_LOG As String = "zones_log"

Private logRows As Collection

Public Sub CleanTaxiZones()
    Dim inputPath As String
    inputPath = FindInputFile()
    If inputPath = "" Then Exit Sub

    On Error GoTo Fail
    Application.ScreenUpdating = False
    Set logRows = New Collection

    Dim lines() As String
    lines = ReadLines(inputPath)

    ' --- header: locate columns by name, whatever their order ---
    Dim header As Variant, iId As Long, iBor As Long, iZone As Long, iSvc As Long
    header = ParseCsvLine(lines(0))
    iId = ColumnIndex(header, "locationid")
    iBor = ColumnIndex(header, "borough")
    iZone = ColumnIndex(header, "zone")
    iSvc = ColumnIndex(header, "service_zone")
    If iId < 0 Or iBor < 0 Or iZone < 0 Or iSvc < 0 Then
        Err.Raise vbObjectError + 1, , "Expected columns LocationID, Borough, Zone, service_zone in " & inputPath
    End If

    ' --- rows ---
    Dim seen As Object, zoneNames As Object
    Set seen = CreateObject("Scripting.Dictionary")
    Set zoneNames = CreateObject("Scripting.Dictionary")

    Dim out() As Variant, nOut As Long, nRead As Long, nRejected As Long, nChanged As Long
    ReDim out(1 To UBound(lines) + 1, 1 To 6)

    Dim i As Long, f As Variant, rawId As String, id As Long
    Dim bor As String, zone As String, svc As String, rule As String
    For i = 1 To UBound(lines)
        If Trim$(lines(i)) <> "" Then
            nRead = nRead + 1
            f = ParseCsvLine(lines(i))
            If UBound(f) < MaxOf(iId, iBor, iZone, iSvc) Then
                AddLog "REJECT", "line " & (i + 1), "", lines(i), "", "missing columns"
                nRejected = nRejected + 1
                GoTo NextRow
            End If

            rawId = CleanSpaces(CStr(f(iId)))
            If Not IsPositiveInteger(rawId) Then
                AddLog "REJECT", "line " & (i + 1), "LocationID", CStr(f(iId)), "", "non-numeric LocationID"
                nRejected = nRejected + 1
                GoTo NextRow
            End If
            id = CLng(rawId)
            If seen.Exists(id) Then
                AddLog "REJECT", CStr(id), "LocationID", CStr(id), "", "duplicate LocationID (first row kept)"
                nRejected = nRejected + 1
                GoTo NextRow
            End If
            seen.Add id, True

            rule = "": bor = CleanBorough(CStr(f(iBor)), rule)
            If LogIfChanged(id, "Borough", CStr(f(iBor)), bor, rule) Then nChanged = nChanged + 1
            rule = "": zone = CleanZone(CStr(f(iZone)), rule)
            If LogIfChanged(id, "Zone", CStr(f(iZone)), zone, rule) Then nChanged = nChanged + 1
            rule = "": svc = CleanServiceZone(CStr(f(iSvc)), rule)
            If LogIfChanged(id, "service_zone", CStr(f(iSvc)), svc, rule) Then nChanged = nChanged + 1

            ' Same zone name on several IDs is legitimate in the TLC file
            ' (e.g. "Corona" = 56 and 57), so it is only reported.
            Dim zoneKey As String: zoneKey = LCase$(bor & "|" & zone)
            If zone <> UNKNOWN_VALUE Then
                If zoneNames.Exists(zoneKey) Then
                    AddLog "WARN", CStr(id), "Zone", zone, "", "same Borough+Zone as LocationID " & zoneNames(zoneKey)
                Else
                    zoneNames.Add zoneKey, id
                End If
            End If

            nOut = nOut + 1
            out(nOut, 1) = id
            out(nOut, 2) = bor
            out(nOut, 3) = zone
            out(nOut, 4) = svc
            out(nOut, 5) = (svc = "Airports" Or svc = "EWR")
            out(nOut, 6) = (bor = UNKNOWN_VALUE)
        End If
NextRow:
    Next i

    If nOut = 0 Then Err.Raise vbObjectError + 2, , "No valid row found in " & inputPath
    ReportMissingIds seen

    WriteCleanSheet out, nOut
    WriteLogSheet

    Dim outputPath As String
    outputPath = Left$(inputPath, InStrRev(inputPath, ".") - 1) & "_clean.csv"
    ExportCsv outputPath, out, nOut

    Application.ScreenUpdating = True
    MsgBox "Rows read: " & nRead & vbLf & _
           "Rows kept: " & nOut & vbLf & _
           "Rows rejected: " & nRejected & vbLf & _
           "Values changed: " & nChanged & vbLf & _
           "Log entries: " & logRows.Count & " (sheet " & SHEET_LOG & ")" & vbLf & vbLf & _
           "Exported: " & outputPath, vbInformation, "Taxi Zone Lookup cleaned"
    Exit Sub

Fail:
    Application.ScreenUpdating = True
    MsgBox "Cleaning failed: " & Err.Description, vbCritical, "TaxiZoneCleaner"
End Sub

' ---------------------------------------------------------------- cleaning

Private Function CleanBorough(ByVal raw As String, ByRef rule As String) As String
    Dim s As String: s = CleanSpaces(raw)
    If s <> raw Then AddRule rule, "whitespace"
    Dim c As String
    Select Case LCase$(s)
        Case "manhattan": c = "Manhattan"
        Case "brooklyn": c = "Brooklyn"
        Case "queens": c = "Queens"
        Case "bronx", "the bronx": c = "Bronx"
        Case "staten island", "staten is", "staten is.": c = "Staten Island"
        Case "ewr", "newark": c = "EWR"
        Case Else
            If IsUnknownValue(s) Then c = UNKNOWN_VALUE Else c = s
    End Select
    If c <> s Then AddRule rule, IIf(c = UNKNOWN_VALUE, "unknown_value", "borough_alias")
    If Not IsKnownBorough(c) Then AddRule rule, "WARN:unrecognized borough"
    CleanBorough = c
End Function

Private Function CleanServiceZone(ByVal raw As String, ByRef rule As String) As String
    Dim s As String: s = CleanSpaces(raw)
    If s <> raw Then AddRule rule, "whitespace"
    Dim c As String
    Select Case LCase$(s)
        Case "yellow zone", "yellow": c = "Yellow Zone"
        Case "boro zone", "boro": c = "Boro Zone"
        Case "airports", "airport": c = "Airports"
        Case "ewr": c = "EWR"
        Case Else
            If IsUnknownValue(s) Then
                c = UNKNOWN_VALUE
            Else
                c = s
                AddRule rule, "WARN:unrecognized service_zone"
            End If
    End Select
    If c <> s Then AddRule rule, IIf(c = UNKNOWN_VALUE, "unknown_value", "service_zone_alias")
    CleanServiceZone = c
End Function

Private Function CleanZone(ByVal raw As String, ByRef rule As String) As String
    Dim s As String: s = CleanSpaces(raw)
    If s <> raw Then AddRule rule, "whitespace"

    Dim t As String: t = s
    Do While InStr(t, " /") > 0: t = Replace(t, " /", "/"): Loop
    Do While InStr(t, "/ ") > 0: t = Replace(t, "/ ", "/"): Loop
    Do While InStr(t, " - ") > 0: t = Replace(t, " - ", "-"): Loop
    If t <> s Then AddRule rule, "separator_spacing"
    s = t

    If IsUnknownValue(s) Then
        If s <> UNKNOWN_VALUE Then AddRule rule, "unknown_value"
        CleanZone = UNKNOWN_VALUE
        Exit Function
    End If

    ' Only fix casing that is clearly broken (ALL CAPS or all lower):
    ' mixed names such as "JFK Airport" or "LaGuardia Airport" stay as they are.
    If s = UCase$(s) Or s = LCase$(s) Then
        t = StrConv(s, vbProperCase)
        If t <> s Then AddRule rule, "casing": s = t
    End If
    CleanZone = s
End Function

Private Function CleanSpaces(ByVal s As String) As String
    s = Replace(s, ChrW(160), " ")
    s = Replace(s, vbTab, " ")
    s = Trim$(s)
    Do While InStr(s, "  ") > 0: s = Replace(s, "  ", " "): Loop
    CleanSpaces = s
End Function

Private Function IsUnknownValue(ByVal s As String) As Boolean
    Select Case LCase$(Trim$(s))
        Case "", "unknown", "n/a", "na", "null", "none", "-", "?": IsUnknownValue = True
    End Select
End Function

Private Function IsKnownBorough(ByVal s As String) As Boolean
    Select Case s
        Case "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", UNKNOWN_VALUE
            IsKnownBorough = True
    End Select
End Function

Private Function IsPositiveInteger(ByVal s As String) As Boolean
    IsPositiveInteger = (Len(s) > 0 And Len(s) <= 9 And Not s Like "*[!0-9]*")
    If IsPositiveInteger Then IsPositiveInteger = (CLng(s) > 0)
End Function

' ---------------------------------------------------------------- logging

Private Sub AddRule(ByRef rule As String, ByVal name As String)
    If rule = "" Then rule = name Else rule = rule & ", " & name
End Sub

' Logs a CHANGE when the value moved, and a WARN for rules flagged "WARN:".
Private Function LogIfChanged(ByVal id As Long, ByVal col As String, ByVal before As String, _
                              ByVal after As String, ByVal rule As String) As Boolean
    Dim parts() As String, p As Variant, changeRules As String
    If rule = "" Then Exit Function
    parts = Split(rule, ", ")
    For Each p In parts
        If Left$(p, 5) = "WARN:" Then
            AddLog "WARN", CStr(id), col, before, "", Mid$(p, 6)
        Else
            AddRule changeRules, CStr(p)
        End If
    Next p
    If before <> after Then
        AddLog "CHANGE", CStr(id), col, before, after, changeRules
        LogIfChanged = True
    End If
End Function

Private Sub AddLog(ByVal severity As String, ByVal id As String, ByVal col As String, _
                   ByVal before As String, ByVal after As String, ByVal rule As String)
    logRows.Add Array(severity, id, col, before, after, rule)
End Sub

' Reports gaps in the LocationID sequence as ranges ("6, 8-131").
Private Sub ReportMissingIds(ByVal seen As Object)
    Dim k As Variant, maxId As Long, id As Long, gapStart As Long, gaps As String
    For Each k In seen.Keys
        If k > maxId Then maxId = k
    Next k
    For id = 1 To maxId + 1
        If id <= maxId And Not seen.Exists(id) Then
            If gapStart = 0 Then gapStart = id
        ElseIf gapStart > 0 Then
            If gaps <> "" Then gaps = gaps & ", "
            If gapStart = id - 1 Then gaps = gaps & gapStart Else gaps = gaps & gapStart & "-" & (id - 1)
            gapStart = 0
        End If
    Next id
    If gaps <> "" Then AddLog "WARN", "", "LocationID", gaps, "", "missing LocationIDs between 1 and " & maxId
End Sub

' ---------------------------------------------------------------- output

Private Sub WriteCleanSheet(ByRef out() As Variant, ByVal nOut As Long)
    Dim ws As Worksheet: Set ws = ResetSheet(SHEET_CLEAN)
    ws.Range("A1:F1").Value = Array("location_id", "borough", "zone", "service_zone", "is_airport", "is_unknown")
    ' Text format first, otherwise Excel may turn a zone name into a date.
    ws.Range("B:D").NumberFormat = "@"
    ws.Range("A2").Resize(nOut, 6).Value = out
    ws.Range("A1:F1").Font.Bold = True
    ws.ListObjects.Add(xlSrcRange, ws.Range("A1").Resize(nOut + 1, 6), , xlYes).Name = "tbl_zones_clean"
    ws.Columns("A:F").AutoFit
End Sub

Private Sub WriteLogSheet()
    Dim ws As Worksheet: Set ws = ResetSheet(SHEET_LOG)
    ws.Range("A1:F1").Value = Array("severity", "location_id", "column", "before", "after", "rule")
    ws.Range("A1:F1").Font.Bold = True
    ws.Columns("A:F").NumberFormat = "@"
    If logRows.Count = 0 Then
        ws.Range("A2").Value = "No change: the file was already clean."
    Else
        Dim data() As Variant, r As Long, c As Long, row As Variant
        ReDim data(1 To logRows.Count, 1 To 6)
        For r = 1 To logRows.Count
            row = logRows(r)
            For c = 0 To 5: data(r, c + 1) = row(c): Next c
        Next r
        ws.Range("A2").Resize(logRows.Count, 6).Value = data
    End If
    ws.Columns("A:F").AutoFit
End Sub

Private Function ResetSheet(ByVal name As String) As Worksheet
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = ThisWorkbook.Worksheets(name)
    On Error GoTo 0
    If ws Is Nothing Then
        Set ws = ThisWorkbook.Worksheets.Add(After:=ThisWorkbook.Worksheets(ThisWorkbook.Worksheets.Count))
        ws.Name = name
    Else
        Do While ws.ListObjects.Count > 0: ws.ListObjects(1).Unlist: Loop
        ws.Cells.Clear
    End If
    Set ResetSheet = ws
End Function

Private Sub ExportCsv(ByVal path As String, ByRef out() As Variant, ByVal nOut As Long)
    Dim sb As String, r As Long
    sb = "location_id,borough,zone,service_zone,is_airport,is_unknown" & vbLf
    For r = 1 To nOut
        sb = sb & out(r, 1) & "," & CsvQuote(CStr(out(r, 2))) & "," & CsvQuote(CStr(out(r, 3))) & "," & _
             CsvQuote(CStr(out(r, 4))) & "," & IIf(out(r, 5), "true", "false") & "," & _
             IIf(out(r, 6), "true", "false") & vbLf
    Next r
    WriteUtf8NoBom path, sb
End Sub

Private Function CsvQuote(ByVal s As String) As String
    If InStr(s, ",") > 0 Or InStr(s, """") > 0 Or InStr(s, vbLf) > 0 Then
        CsvQuote = """" & Replace(s, """", """""") & """"
    Else
        CsvQuote = s
    End If
End Function

' ---------------------------------------------------------------- file I/O

Private Function FindInputFile() As String
    Dim candidates As Variant, c As Variant, picked As Variant
    candidates = Array(ThisWorkbook.Path & "\..\data\" & DEFAULT_FILE, _
                       ThisWorkbook.Path & "\data\" & DEFAULT_FILE, _
                       ThisWorkbook.Path & "\" & DEFAULT_FILE)
    For Each c In candidates
        If Len(ThisWorkbook.Path) > 0 And Dir$(CStr(c)) <> "" Then
            FindInputFile = CStr(c)
            Exit Function
        End If
    Next c
    picked = Application.GetOpenFilename("CSV files (*.csv),*.csv", , "Select taxi_zone_lookup.csv")
    If VarType(picked) = vbString Then FindInputFile = picked
End Function

Private Function ReadLines(ByVal path As String) As String()
    Dim st As Object, text As String
    Set st = CreateObject("ADODB.Stream")
    st.Type = 2            ' text
    st.Charset = "utf-8"
    st.Open
    st.LoadFromFile path
    text = st.ReadText(-1) ' all
    st.Close
    If Left$(text, 1) = ChrW(&HFEFF) Then text = Mid$(text, 2)
    text = Replace(text, vbCrLf, vbLf)
    text = Replace(text, vbCr, vbLf)
    If Trim$(Replace(text, vbLf, "")) = "" Then Err.Raise vbObjectError + 3, , "Empty file: " & path
    ReadLines = Split(text, vbLf)
End Function

Private Sub WriteUtf8NoBom(ByVal path As String, ByVal text As String)
    Dim st As Object, bin As Object
    Set st = CreateObject("ADODB.Stream")
    st.Type = 2
    st.Charset = "utf-8"
    st.Open
    st.WriteText text
    st.Position = 3        ' skip the BOM ADODB always writes
    Set bin = CreateObject("ADODB.Stream")
    bin.Type = 1           ' binary
    bin.Open
    st.CopyTo bin
    bin.SaveToFile path, 2 ' overwrite
    bin.Close
    st.Close
End Sub

' RFC 4180 style: handles quoted fields, commas and "" inside quotes.
' (Line breaks inside a quoted field are not supported: the TLC file has none.)
Private Function ParseCsvLine(ByVal s As String) As Variant
    Dim fields() As String, n As Long, i As Long, ch As String, cur As String, inQuotes As Boolean
    i = 1
    Do While i <= Len(s)
        ch = Mid$(s, i, 1)
        If inQuotes Then
            If ch = """" Then
                If Mid$(s, i + 1, 1) = """" Then
                    cur = cur & """": i = i + 1
                Else
                    inQuotes = False
                End If
            Else
                cur = cur & ch
            End If
        ElseIf ch = """" Then
            inQuotes = True
        ElseIf ch = "," Then
            ReDim Preserve fields(0 To n): fields(n) = cur: n = n + 1: cur = ""
        Else
            cur = cur & ch
        End If
        i = i + 1
    Loop
    ReDim Preserve fields(0 To n): fields(n) = cur
    ParseCsvLine = fields
End Function

Private Function ColumnIndex(ByVal header As Variant, ByVal name As String) As Long
    Dim i As Long
    For i = 0 To UBound(header)
        If LCase$(CleanSpaces(Replace(CStr(header(i)), ChrW(&HFEFF), ""))) = name Then
            ColumnIndex = i
            Exit Function
        End If
    Next i
    ColumnIndex = -1
End Function

Private Function MaxOf(ParamArray v() As Variant) As Long
    Dim x As Variant
    For Each x In v
        If x > MaxOf Then MaxOf = x
    Next x
End Function
