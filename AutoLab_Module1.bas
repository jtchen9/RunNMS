Option Explicit

' AutoLab DemoRoom mobility XLSM helper macros ¡X Phase 10a
'
' This module is designed to be pasted into a normal VBA module such as Module1.
' Do NOT include an "Attribute VB_Name = ..." line when copy/pasting manually.
'
' Rule authority:
'   These Excel macros are authoring/export/preflight helpers only.
'   CommonCheckers remains the official PASS/FAIL validation engine.

Private Const SHEET_COMMAND As String = "CommandSheet"
Private Const SHEET_POSES As String = "InitialPoses"
Private Const SHEET_STATIC_MAP As String = "StaticMap"
Private Const SHEET_MAP As String = "Map"
Private Const SHEET_EXPORT As String = "ExportPreview"
Private Const SHEET_INITIAL_EXPORT As String = "InitialPosesExportPreview"
Private Const SHEET_PREFLIGHT_CONFIG As String = "PreflightConfig"
Private Const SHEET_PREFLIGHT_STATUS As String = "PreflightStatus"
Private Const SHEET_VALIDATION_REPORT As String = "ValidationReport"

Private Const MAP_TOP As Long = 4
Private Const MAP_LEFT As Long = 3
Private Const MAP_ROWS As Long = 114
Private Const MAP_COLS As Long = 114
Private Const CELL_M As Double = 0.1
Private Const ROBOT_RADIUS_M As Double = 0.6
Private Const MOVE_X_MIN_M As Double = 1.4
Private Const MOVE_X_MAX_EXCLUSIVE_M As Double = 10.1
Private Const MOVE_Y_MIN_M As Double = 0.3
Private Const MOVE_Y_MAX_EXCLUSIVE_M As Double = 11.1
Private Const MOVE_HEADING_MIN_DEG As Double = 0#
Private Const MOVE_HEADING_MAX_EXCLUSIVE_DEG As Double = 360#

' CommandSheet columns
Private Const COL_CMD_ID As Long = 1
Private Const COL_LINE_TYPE As Long = 2
Private Const COL_ENABLED As Long = 3
Private Const COL_MINUTE As Long = 4
Private Const COL_SECOND As Long = 5
Private Const COL_SCANNER As Long = 6
Private Const COL_COMMAND_ID As Long = 7
Private Const COL_CATEGORY As Long = 8
Private Const COL_ACTION As Long = 9
Private Const COL_PARAM1 As Long = 10   ' x_m for mobility.move
Private Const COL_PARAM2 As Long = 11   ' y_m for mobility.move
Private Const COL_PARAM3 As Long = 12   ' heading_deg for mobility.move
Private Const COL_PARAM4 As Long = 13
Private Const COL_PARAM5 As Long = 14
Private Const COL_PARAM6 As Long = 15
Private Const COL_ARGS_JSON As Long = 16
Private Const COL_STATUS As Long = 17
Private Const COL_ISSUE_CODE As Long = 18
Private Const COL_MESSAGE As Long = 19
Private Const COL_SUGGESTION As Long = 20

' First row below the CommandSheet header row. This includes Key/Value command rows.
Private Const FIRST_DATA_ROW As Long = 4

Private Const COMMAND_GUI_MAX_ROW As Long = 200
Private Const CMD_LIST_CSV As String = "mobility.report.location,mobility.move,mobility.in2out,mobility.out2in,scan.start,scan.stop,scan.once"

' ============================================================
' Phase-3 map macros
' ============================================================

Public Sub RefreshMapAtSelectedCommandRow()
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Dim selectedCmdRowID As Long
    Dim selectedValueRow As Long

    selectedValueRow = ResolveSelectedCommandValueRow(wsCmd)
    If selectedValueRow = 0 Then
        MsgBox "Please select a Key or Value row in CommandSheet first.", vbExclamation
        Exit Sub
    End If

    selectedCmdRowID = CLng(wsCmd.Cells(selectedValueRow, COL_CMD_ID).value)
    If selectedCmdRowID <= 0 Then
        MsgBox "Could not resolve CmdRowID from the selected row.", vbExclamation
        Exit Sub
    End If

    Dim invalidMoveRow As Long
    Dim validationMessage As String
    If Not ValidateMobilityMovesForMap(wsCmd, selectedCmdRowID, selectedValueRow, invalidMoveRow, validationMessage) Then
        MsgBox "Map refresh stopped at CommandSheet row " & invalidMoveRow & "." & vbCrLf & _
               validationMessage, vbExclamation
        Exit Sub
    End If

    Application.ScreenUpdating = False

    RestoreMapFromStatic
    DrawPlannedOverlay selectedCmdRowID, selectedValueRow

    ThisWorkbook.Worksheets(SHEET_MAP).Activate
    Application.ScreenUpdating = True

    MsgBox "Map refreshed at CmdRowID " & selectedCmdRowID & ".", vbInformation
End Sub

Private Function ValidateMobilityMovesForMap( _
    wsCmd As Worksheet, _
    ByVal selectedCmdRowID As Long, _
    ByVal selectedValueRow As Long, _
    ByRef invalidMoveRow As Long, _
    ByRef validationMessage As String) As Boolean

    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    Dim r As Long
    Dim cmdID As Long
    Dim action As String
    Dim shouldValidate As Boolean
    Dim xM As Double, yM As Double
    Dim hasHeading As Boolean, headingDeg As Double
    Dim issueCode As String, issueMessage As String, issueSuggestion As String

    For r = FIRST_DATA_ROW To lastRow
        shouldValidate = False

        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Then
            If r = selectedValueRow Then
                shouldValidate = True
            ElseIf IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) And IsNumeric(wsCmd.Cells(r, COL_CMD_ID).value) Then
                cmdID = CLng(wsCmd.Cells(r, COL_CMD_ID).value)
                shouldValidate = (cmdID > 0 And cmdID < selectedCmdRowID)
            End If
        End If

        If shouldValidate Then
            action = Trim$(CStr(wsCmd.Cells(r, COL_ACTION).value))
            If action = "" Then
                action = Trim$(CStr(wsCmd.Cells(r, COL_COMMAND_ID).value))
            End If

            If action = "mobility.move" Then
                If Not ValidateMobilityMoveRow(wsCmd, r, xM, yM, hasHeading, headingDeg, _
                                               issueCode, issueMessage, issueSuggestion) Then
                    SetMobilityMoveValidationError wsCmd, r, issueCode, issueMessage, issueSuggestion
                    invalidMoveRow = r
                    validationMessage = issueMessage
                    ValidateMobilityMovesForMap = False
                    Exit Function
                End If
            End If
        End If
    Next r

    ValidateMobilityMovesForMap = True
End Function

Public Sub ClearMapToStatic()
    Application.ScreenUpdating = False
    RestoreMapFromStatic
    Application.ScreenUpdating = True
End Sub

Private Function ResolveSelectedCommandValueRow(wsCmd As Worksheet) As Long
    Dim r As Long
    r = ActiveCell.Row

    If ActiveSheet.Name <> SHEET_COMMAND Then
        ResolveSelectedCommandValueRow = 0
        Exit Function
    End If

    Dim lineType As String
    lineType = LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value)))

    If lineType = "value" Then
        ResolveSelectedCommandValueRow = r
    ElseIf lineType = "key" Then
        If LCase$(Trim$(CStr(wsCmd.Cells(r + 1, COL_LINE_TYPE).value))) = "value" Then
            ResolveSelectedCommandValueRow = r + 1
        Else
            ResolveSelectedCommandValueRow = 0
        End If
    Else
        ResolveSelectedCommandValueRow = 0
    End If
End Function

Private Sub RestoreMapFromStatic()
    Dim wsStatic As Worksheet
    Dim wsMap As Worksheet
    Set wsStatic = ThisWorkbook.Worksheets(SHEET_STATIC_MAP)
    Set wsMap = ThisWorkbook.Worksheets(SHEET_MAP)

    wsStatic.Range("A1:DQ118").Copy
    wsMap.Range("A1").PasteSpecial xlPasteAll
    Application.CutCopyMode = False
End Sub

Private Sub DrawPlannedOverlay(ByVal selectedCmdRowID As Long, ByVal selectedValueRow As Long)
    Dim wsCmd As Worksheet
    Dim wsPose As Worksheet
    Dim wsMap As Worksheet

    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)
    Set wsPose = ThisWorkbook.Worksheets(SHEET_POSES)
    Set wsMap = ThisWorkbook.Worksheets(SHEET_MAP)

    Dim xDict As Object, yDict As Object, hDict As Object
    Set xDict = CreateObject("Scripting.Dictionary")
    Set yDict = CreateObject("Scripting.Dictionary")
    Set hDict = CreateObject("Scripting.Dictionary")

    LoadInitialPoses wsPose, xDict, yDict, hDict

    Dim selectedScanner As String
    Dim selectedAction As String
    Dim selectedIsMovement As Boolean
    Dim selectedIsMacro As Boolean
    Dim selectedStartX As Double, selectedStartY As Double
    Dim selectedTargetX As Double, selectedTargetY As Double
    Dim macroPolicyStartX As Double, macroPolicyStartY As Double
    Dim macroStartErrorM As Double
    Dim hasPlannedCurrent As Boolean

    selectedScanner = CStr(wsCmd.Cells(selectedValueRow, COL_SCANNER).value)
    selectedAction = Trim$(CStr(wsCmd.Cells(selectedValueRow, COL_ACTION).value))
    If selectedAction = "" Then
        selectedAction = Trim$(CStr(wsCmd.Cells(selectedValueRow, COL_COMMAND_ID).value))
    End If

    selectedIsMovement = False
    selectedIsMacro = IsMacroCommand(selectedAction)

    ' Phase-9c2:
    '   Simulate only commands BEFORE the selected command.
    '   A macro path starts from the robot's planned current pose, not from
    '   the policy start point. The policy start point is only the admission
    '   center used for tolerance checking.
    ApplyCommandsBeforeSelected wsCmd, selectedCmdRowID, xDict, yDict, hDict

    hasPlannedCurrent = xDict.Exists(selectedScanner)

    If selectedAction = "mobility.move" And hasPlannedCurrent Then
        If IsNumeric(wsCmd.Cells(selectedValueRow, COL_PARAM1).value) And IsNumeric(wsCmd.Cells(selectedValueRow, COL_PARAM2).value) Then
            selectedIsMovement = True
            selectedStartX = CDbl(xDict(selectedScanner))
            selectedStartY = CDbl(yDict(selectedScanner))
            selectedTargetX = CDbl(wsCmd.Cells(selectedValueRow, COL_PARAM1).value)
            selectedTargetY = CDbl(wsCmd.Cells(selectedValueRow, COL_PARAM2).value)
        End If
    ElseIf selectedIsMacro And hasPlannedCurrent Then
        selectedStartX = CDbl(xDict(selectedScanner))
        selectedStartY = CDbl(yDict(selectedScanner))
        If GetMacroEndpointFromCurrent(selectedAction, selectedStartX, selectedStartY, selectedTargetX, selectedTargetY) Then
            selectedIsMovement = True
            If GetMacroPolicyStart(selectedAction, macroPolicyStartX, macroPolicyStartY) Then
                macroStartErrorM = Sqr((selectedStartX - macroPolicyStartX) ^ 2 + (selectedStartY - macroPolicyStartY) ^ 2)
            End If
        End If
    End If

    ' Draw dynamic safety zones using planned positions before the selected command.
    ' If the selected row is a movement command, do not draw the selected robot's
    ' own dynamic zone. A robot should not be blocked by its own safety zone.
    Dim scanner As Variant
    For Each scanner In xDict.Keys
        If Not (selectedIsMovement And CStr(scanner) = selectedScanner) Then
            DrawRobotDynamicZone wsMap, CDbl(xDict(scanner)), CDbl(yDict(scanner))
        End If
    Next scanner

    If selectedIsMovement Then
        DrawSelectedPath wsMap, selectedStartX, selectedStartY, selectedTargetX, selectedTargetY, selectedIsMacro, selectedScanner, xDict, yDict
        DrawTargetCell wsMap, selectedTargetX, selectedTargetY, RobotShortLabel(selectedScanner), selectedIsMacro
    End If

    For Each scanner In xDict.Keys
        DrawRobotCenter wsMap, CDbl(xDict(scanner)), CDbl(yDict(scanner)), RobotShortLabel(CStr(scanner))
    Next scanner

    WriteMapStatus wsMap, selectedCmdRowID, selectedScanner, selectedIsMovement, selectedIsMacro, selectedAction, _
                   selectedStartX, selectedStartY, selectedTargetX, selectedTargetY, _
                   macroPolicyStartX, macroPolicyStartY, macroStartErrorM
End Sub

Private Sub LoadInitialPoses(wsPose As Worksheet, ByRef xDict As Object, ByRef yDict As Object, ByRef hDict As Object)
    Dim lastRow As Long
    lastRow = wsPose.Cells(wsPose.rows.Count, 2).End(xlUp).Row

    Dim r As Long, scanner As String
    For r = 4 To lastRow
        If IsTruthy(wsPose.Cells(r, 1).value) Then
            scanner = Trim$(CStr(wsPose.Cells(r, 2).value))
            If scanner <> "" Then
                xDict(scanner) = CDbl(wsPose.Cells(r, 3).value)
                yDict(scanner) = CDbl(wsPose.Cells(r, 4).value)
                hDict(scanner) = CDbl(wsPose.Cells(r, 5).value)
            End If
        End If
    Next r
End Sub

Private Sub ApplyCommandsUpToSelected(wsCmd As Worksheet, ByVal selectedCmdRowID As Long, ByRef xDict As Object, ByRef yDict As Object, ByRef hDict As Object)
    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    Dim r As Long
    Dim cmdID As Long
    Dim lineType As String
    Dim scanner As String
    Dim action As String

    For r = 4 To lastRow
        lineType = LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value)))

        If lineType = "value" And IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) Then
            If IsNumeric(wsCmd.Cells(r, COL_CMD_ID).value) Then
                cmdID = CLng(wsCmd.Cells(r, COL_CMD_ID).value)
            Else
                cmdID = 0
            End If

            If cmdID > 0 And cmdID <= selectedCmdRowID Then
                scanner = Trim$(CStr(wsCmd.Cells(r, COL_SCANNER).value))
                action = Trim$(CStr(wsCmd.Cells(r, COL_ACTION).value))
                If action = "" Then
                    action = Trim$(CStr(wsCmd.Cells(r, COL_COMMAND_ID).value))
                End If

                If scanner <> "" And xDict.Exists(scanner) Then
                    Select Case action
                        Case "mobility.move"
                            If IsNumeric(wsCmd.Cells(r, COL_PARAM1).value) And IsNumeric(wsCmd.Cells(r, COL_PARAM2).value) Then
                                xDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM1).value)
                                yDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM2).value)
                            End If
                            If IsNumeric(wsCmd.Cells(r, COL_PARAM3).value) Then
                                hDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM3).value)
                            End If

                        Case "mobility.report.location"
                            ' No planned pose change.

                        Case "mobility.in2out", "mobility.out2in"
                            Dim mx0 As Double, my0 As Double, mx1 As Double, my1 As Double, mh As Double
                            If GetMacroEndpointFromCurrent(action, CDbl(xDict(scanner)), CDbl(yDict(scanner)), mx1, my1) Then
                                xDict(scanner) = mx1
                                yDict(scanner) = my1
                                If GetMacroHeading(action, mh) Then
                                    hDict(scanner) = mh
                                End If
                            End If
                    End Select
                End If
            End If
        End If
    Next r
End Sub


Private Sub ApplyCommandsBeforeSelected(wsCmd As Worksheet, ByVal selectedCmdRowID As Long, ByRef xDict As Object, ByRef yDict As Object, ByRef hDict As Object)
    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    Dim r As Long
    Dim cmdID As Long
    Dim lineType As String
    Dim scanner As String
    Dim action As String

    For r = 4 To lastRow
        lineType = LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value)))

        If lineType = "value" And IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) Then
            If IsNumeric(wsCmd.Cells(r, COL_CMD_ID).value) Then
                cmdID = CLng(wsCmd.Cells(r, COL_CMD_ID).value)
            Else
                cmdID = 0
            End If

            If cmdID > 0 And cmdID < selectedCmdRowID Then
                scanner = Trim$(CStr(wsCmd.Cells(r, COL_SCANNER).value))
                action = Trim$(CStr(wsCmd.Cells(r, COL_ACTION).value))
                If action = "" Then
                    action = Trim$(CStr(wsCmd.Cells(r, COL_COMMAND_ID).value))
                End If

                If scanner <> "" And xDict.Exists(scanner) Then
                    Select Case action
                        Case "mobility.move"
                            If IsNumeric(wsCmd.Cells(r, COL_PARAM1).value) And IsNumeric(wsCmd.Cells(r, COL_PARAM2).value) Then
                                xDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM1).value)
                                yDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM2).value)
                            End If
                            If IsNumeric(wsCmd.Cells(r, COL_PARAM3).value) Then
                                hDict(scanner) = CDbl(wsCmd.Cells(r, COL_PARAM3).value)
                            End If

                        Case "mobility.report.location"
                            ' No planned pose change.

                        Case "mobility.in2out", "mobility.out2in"
                            Dim ex As Double, ey As Double, mh As Double
                            If GetMacroEndpointFromCurrent(action, CDbl(xDict(scanner)), CDbl(yDict(scanner)), ex, ey) Then
                                xDict(scanner) = ex
                                yDict(scanner) = ey
                                If GetMacroHeading(action, mh) Then
                                    hDict(scanner) = mh
                                End If
                            End If
                    End Select
                End If
            End If
        End If
    Next r
End Sub


Private Sub DrawRobotDynamicZone(wsMap As Worksheet, ByVal xM As Double, ByVal yM As Double)
    Dim centerRow As Long, centerCol As Long
    XYToGrid xM, yM, centerRow, centerCol

    Dim radiusCells As Long
    radiusCells = CLng(Application.WorksheetFunction.Ceiling(ROBOT_RADIUS_M / CELL_M, 1))

    Dim rr As Long, cc As Long
    Dim cx As Double, cy As Double
    Dim cell As Range

    For rr = centerRow - radiusCells To centerRow + radiusCells
        For cc = centerCol - radiusCells To centerCol + radiusCells
            If InGrid(rr, cc) Then
                cx = (cc + 0.5) * CELL_M
                cy = (rr + 0.5) * CELL_M
                If Sqr((cx - xM) ^ 2 + (cy - yM) ^ 2) <= ROBOT_RADIUS_M Then
                    Set cell = GridCell(wsMap, rr, cc)
                    If Not IsStaticRestrictedCell(cell) And Not IsBumpGuardCell(cell) Then
                        cell.Interior.Color = RGB(157, 195, 230)
                    End If
                End If
            End If
        Next cc
    Next rr
End Sub

Private Sub DrawSelectedPath(wsMap As Worksheet, ByVal x0 As Double, ByVal y0 As Double, ByVal x1 As Double, ByVal y1 As Double, _
                             ByVal allowBumpGuard As Boolean, ByVal movingScanner As String, ByRef xDict As Object, ByRef yDict As Object)
    Dim dist As Double
    dist = Sqr((x1 - x0) ^ 2 + (y1 - y0) ^ 2)

    Dim steps As Long
    steps = CLng(Application.WorksheetFunction.Max(1, Application.WorksheetFunction.Ceiling(dist / 0.05, 1)))

    Dim i As Long
    Dim t As Double, x As Double, y As Double
    Dim rr As Long, cc As Long
    Dim cell As Range
    Dim robotConflict As Boolean

    For i = 0 To steps
        t = i / steps
        x = x0 + t * (x1 - x0)
        y = y0 + t * (y1 - y0)

        XYToGrid x, y, rr, cc
        If InGrid(rr, cc) Then
            Set cell = GridCell(wsMap, rr, cc)

            robotConflict = PathSampleTooCloseToOtherRobot(x, y, movingScanner, xDict, yDict)

            If IsStaticRestrictedCell(cell) Or IsDynamicZoneCell(cell) Or robotConflict Or ((Not allowBumpGuard) And IsBumpGuardCell(cell)) Then
                cell.Interior.Color = RGB(112, 48, 160)
                cell.value = "!"
                cell.Font.Color = RGB(255, 255, 255)
                cell.Font.Bold = True
            Else
                cell.Interior.Color = RGB(0, 176, 80)
            End If
        End If
    Next i
End Sub

Private Function PathSampleTooCloseToOtherRobot(ByVal x As Double, ByVal y As Double, ByVal movingScanner As String, _
                                                ByRef xDict As Object, ByRef yDict As Object) As Boolean
    Dim scanner As Variant
    For Each scanner In xDict.Keys
        If CStr(scanner) <> movingScanner Then
            If Sqr((x - CDbl(xDict(scanner))) ^ 2 + (y - CDbl(yDict(scanner))) ^ 2) <= ROBOT_RADIUS_M Then
                PathSampleTooCloseToOtherRobot = True
                Exit Function
            End If
        End If
    Next scanner

    PathSampleTooCloseToOtherRobot = False
End Function

Private Sub DrawTargetCell(wsMap As Worksheet, ByVal xM As Double, ByVal yM As Double, ByVal label As String, ByVal allowBumpGuard As Boolean)
    Dim rr As Long, cc As Long
    XYToGrid xM, yM, rr, cc

    If InGrid(rr, cc) Then
        With GridCell(wsMap, rr, cc)
            If IsStaticRestrictedCell(GridCell(wsMap, rr, cc)) Or ((Not allowBumpGuard) And IsBumpGuardCell(GridCell(wsMap, rr, cc))) Then
                .Interior.Color = RGB(112, 48, 160)
                .value = "!"
            Else
                .Interior.Color = RGB(0, 176, 80)
                .value = label
            End If
            .Font.Color = RGB(255, 255, 255)
            .Font.Bold = True
            .HorizontalAlignment = xlCenter
            .VerticalAlignment = xlCenter
        End With
    End If
End Sub

Private Sub DrawRobotCenter(wsMap As Worksheet, ByVal xM As Double, ByVal yM As Double, ByVal label As String)
    Dim rr As Long, cc As Long
    XYToGrid xM, yM, rr, cc

    If InGrid(rr, cc) Then
        With GridCell(wsMap, rr, cc)
            .Interior.Color = RGB(31, 78, 121)
            .value = label
            .Font.Color = RGB(255, 255, 255)
            .Font.Bold = True
            .HorizontalAlignment = xlCenter
            .VerticalAlignment = xlCenter
        End With
    End If
End Sub

Private Sub WriteMapStatus(wsMap As Worksheet, ByVal cmdRowId As Long, ByVal scanner As String, ByVal isMovement As Boolean, _
                           ByVal isMacro As Boolean, ByVal action As String, _
                           ByVal sx As Double, ByVal sy As Double, ByVal tx As Double, ByVal ty As Double, _
                           ByVal policyStartX As Double, ByVal policyStartY As Double, ByVal startErrorM As Double)
    wsMap.Range("DO10:DR20").ClearContents
    wsMap.Range("DO10:DR10").value = Array("Phase-9c2 map preview", "", "", "")
    wsMap.Range("DO10:DR10").Interior.Color = RGB(31, 78, 121)
    wsMap.Range("DO10:DR10").Font.Color = RGB(255, 255, 255)
    wsMap.Range("DO10:DR10").Font.Bold = True

    wsMap.Range("DO11").value = "Selected CmdRowID"
    wsMap.Range("DP11").value = cmdRowId
    wsMap.Range("DO12").value = "Selected robot"
    wsMap.Range("DP12").value = scanner
    wsMap.Range("DO13").value = "Action"
    wsMap.Range("DP13").value = action

    If isMovement Then
        wsMap.Range("DO14").value = "Actual planned path"
        wsMap.Range("DP14").value = "(" & sx & ", " & sy & ") -> (" & tx & ", " & ty & ")"
        wsMap.Range("DO15").value = "Path type"
        If isMacro Then
            wsMap.Range("DP15").value = "Macro relative path; bump guard allowed"
            wsMap.Range("DO16").value = "Macro policy start"
            wsMap.Range("DP16").value = "(" & policyStartX & ", " & policyStartY & ")"
            wsMap.Range("DO17").value = "Macro start error"
            wsMap.Range("DP17").value = Format(startErrorM, "0.000") & " m"
        Else
            wsMap.Range("DP15").value = "Normal move; bump guard restricted"
            wsMap.Range("DO16").value = "Macro policy start"
            wsMap.Range("DP16").value = "Not applicable"
            wsMap.Range("DO17").value = "Macro start error"
            wsMap.Range("DP17").value = "Not applicable"
        End If
        wsMap.Range("DO18").value = "Own dynamic zone"
        wsMap.Range("DP18").value = "Skipped for selected robot"
    Else
        wsMap.Range("DO14").value = "Actual planned path"
        wsMap.Range("DP14").value = "No selected movement"
        wsMap.Range("DO15").value = "Path type"
        wsMap.Range("DP15").value = "Not applicable"
        wsMap.Range("DO16").value = "Macro policy start"
        wsMap.Range("DP16").value = "Not applicable"
        wsMap.Range("DO17").value = "Macro start error"
        wsMap.Range("DP17").value = "Not applicable"
        wsMap.Range("DO18").value = "Own dynamic zone"
        wsMap.Range("DP18").value = "Not applicable"
    End If

    wsMap.Range("DO19").value = "Robot radius"
    wsMap.Range("DP19").value = ROBOT_RADIUS_M & " m"
    wsMap.Range("DO20").value = "Rule authority"
    wsMap.Range("DP20").value = "CommonCheckers"
    wsMap.Range("DO11:DR20").WrapText = True
End Sub


' ============================================================
' Phase-4 command authoring macros
' ============================================================

Public Sub BuildArgsJson()
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    Dim r As Long
    Dim builtCount As Long
    Dim errorCount As Long

    Application.ScreenUpdating = False

    For r = 4 To lastRow
        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Then
            If IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) Then
                If BuildArgsJsonForRow(wsCmd, r) Then
                    builtCount = builtCount + 1
                Else
                    errorCount = errorCount + 1
                End If
            Else
                wsCmd.Cells(r, COL_STATUS).value = "DISABLED"
                wsCmd.Cells(r, COL_ISSUE_CODE).ClearContents
                wsCmd.Cells(r, COL_MESSAGE).ClearContents
                wsCmd.Cells(r, COL_SUGGESTION).ClearContents
            End If
        End If
    Next r

    Application.ScreenUpdating = True

    MsgBox "Build Args JSON finished." & vbCrLf & _
           "Built rows: " & builtCount & vbCrLf & _
           "Rows with errors: " & errorCount, _
           IIf(errorCount > 0, vbExclamation, vbInformation)
End Sub

Private Function BuildArgsJsonForRow(wsCmd As Worksheet, ByVal r As Long) As Boolean
    Dim commandId As String
    Dim category As String
    Dim action As String

    commandId = Trim$(CStr(wsCmd.Cells(r, COL_COMMAND_ID).value))
    ClearFeedback wsCmd, r

    If commandId = "" Then
        wsCmd.Cells(r, COL_STATUS).value = "ERROR"
        wsCmd.Cells(r, COL_ISSUE_CODE).value = "UNKNOWN_ACTION"
        wsCmd.Cells(r, COL_MESSAGE).value = "CommandID is blank."
        wsCmd.Cells(r, COL_SUGGESTION).value = "Choose a CommandID from the dropdown list."
        BuildArgsJsonForRow = False
        Exit Function
    End If

    If Not IsSupportedCommandId(commandId) Then
        wsCmd.Cells(r, COL_STATUS).value = "ERROR"
        wsCmd.Cells(r, COL_ISSUE_CODE).value = "UNKNOWN_ACTION"
        wsCmd.Cells(r, COL_MESSAGE).value = "Unsupported CommandID for current robot-script template: " & commandId
        wsCmd.Cells(r, COL_SUGGESTION).value = "Choose a supported command from the dropdown list."
        BuildArgsJsonForRow = False
        Exit Function
    End If

    ' CommandID is the public editable field. Keep backend fields synchronized.
    category = CategoryForCommandId(commandId)
    action = commandId
    wsCmd.Cells(r, COL_CATEGORY).value = category
    wsCmd.Cells(r, COL_ACTION).value = action

    ApplyCommandLayoutToValueRow wsCmd, r

    Select Case action
        Case "mobility.report.location", "mobility.in2out", "mobility.out2in", _
             "scan.start", "scan.stop", "scan.once"
            wsCmd.Cells(r, COL_ARGS_JSON).value = "{}"
            wsCmd.Cells(r, COL_STATUS).value = "OK"
            BuildArgsJsonForRow = True

        Case "mobility.move"
            BuildArgsJsonForRow = BuildMobilityMoveArgs(wsCmd, r)

        Case Else
            wsCmd.Cells(r, COL_STATUS).value = "ERROR"
            wsCmd.Cells(r, COL_ISSUE_CODE).value = "UNKNOWN_ACTION"
            wsCmd.Cells(r, COL_MESSAGE).value = "Unsupported action for current robot-script template: " & action
            wsCmd.Cells(r, COL_SUGGESTION).value = "Choose a supported command from the dropdown list."
            BuildArgsJsonForRow = False
    End Select
End Function

Private Function BuildMobilityMoveArgs(wsCmd As Worksheet, ByVal r As Long) As Boolean
    Dim xM As Double, yM As Double
    Dim hasHeading As Boolean, headingDeg As Double
    Dim issueCode As String, issueMessage As String, issueSuggestion As String

    If Not ValidateMobilityMoveRow(wsCmd, r, xM, yM, hasHeading, headingDeg, _
                                   issueCode, issueMessage, issueSuggestion) Then
        SetMobilityMoveValidationError wsCmd, r, issueCode, issueMessage, issueSuggestion
        BuildMobilityMoveArgs = False
        Exit Function
    End If

    Dim args As String
    args = "{""x_m"":" & JsonNumber(xM) & ",""y_m"":" & JsonNumber(yM)

    If hasHeading Then
        args = args & ",""heading_deg"":" & JsonNumber(headingDeg)
    End If

    args = args & "}"

    wsCmd.Cells(r, COL_ARGS_JSON).value = args
    wsCmd.Cells(r, COL_STATUS).value = "OK"
    BuildMobilityMoveArgs = True
End Function

Private Function ValidateMobilityMoveRow( _
    wsCmd As Worksheet, _
    ByVal r As Long, _
    ByRef xM As Double, _
    ByRef yM As Double, _
    ByRef hasHeading As Boolean, _
    ByRef headingDeg As Double, _
    ByRef issueCode As String, _
    ByRef issueMessage As String, _
    ByRef issueSuggestion As String) As Boolean

    Dim xVal As Variant, yVal As Variant, hVal As Variant
    xVal = wsCmd.Cells(r, COL_PARAM1).value
    yVal = wsCmd.Cells(r, COL_PARAM2).value
    hVal = wsCmd.Cells(r, COL_PARAM3).value

    If IsError(xVal) Then
        issueCode = "COMMAND_ARG_BAD_TYPE"
        issueMessage = "mobility.move x_m contains an Excel error."
        issueSuggestion = "Enter x_m as a number in the range [1.4, 10.1)."
        Exit Function
    End If

    If IsBlankCellValue(xVal) Then
        issueCode = "COMMAND_ARGS_MISSING_REQUIRED"
        issueMessage = "mobility.move requires x_m. The x_m cell is blank."
        issueSuggestion = "Enter x_m in the range [1.4, 10.1)."
        Exit Function
    End If

    If Not IsNumeric(xVal) Then
        issueCode = "COMMAND_ARG_BAD_TYPE"
        issueMessage = "mobility.move x_m must be numeric."
        issueSuggestion = "Enter x_m as a number in the range [1.4, 10.1)."
        Exit Function
    End If

    xM = CDbl(xVal)
    If xM < MOVE_X_MIN_M Or xM >= MOVE_X_MAX_EXCLUSIVE_M Then
        issueCode = "COMMAND_ARG_OUT_OF_RANGE"
        issueMessage = "mobility.move x_m is outside the allowed range [1.4, 10.1)."
        issueSuggestion = "Enter x_m greater than or equal to 1.4 and less than 10.1."
        Exit Function
    End If

    If IsError(yVal) Then
        issueCode = "COMMAND_ARG_BAD_TYPE"
        issueMessage = "mobility.move y_m contains an Excel error."
        issueSuggestion = "Enter y_m as a number in the range [0.3, 11.1)."
        Exit Function
    End If

    If IsBlankCellValue(yVal) Then
        issueCode = "COMMAND_ARGS_MISSING_REQUIRED"
        issueMessage = "mobility.move requires y_m. The y_m cell is blank."
        issueSuggestion = "Enter y_m in the range [0.3, 11.1)."
        Exit Function
    End If

    If Not IsNumeric(yVal) Then
        issueCode = "COMMAND_ARG_BAD_TYPE"
        issueMessage = "mobility.move y_m must be numeric."
        issueSuggestion = "Enter y_m as a number in the range [0.3, 11.1)."
        Exit Function
    End If

    yM = CDbl(yVal)
    If yM < MOVE_Y_MIN_M Or yM >= MOVE_Y_MAX_EXCLUSIVE_M Then
        issueCode = "COMMAND_ARG_OUT_OF_RANGE"
        issueMessage = "mobility.move y_m is outside the allowed range [0.3, 11.1)."
        issueSuggestion = "Enter y_m greater than or equal to 0.3 and less than 11.1."
        Exit Function
    End If

    If IsError(hVal) Then
        issueCode = "COMMAND_ARG_BAD_TYPE"
        issueMessage = "mobility.move heading_deg contains an Excel error."
        issueSuggestion = "Clear heading_deg or enter a number in the range [0, 360)."
        Exit Function
    End If

    hasHeading = Not IsBlankCellValue(hVal)
    If hasHeading Then
        If Not IsNumeric(hVal) Then
            issueCode = "COMMAND_ARG_BAD_TYPE"
            issueMessage = "mobility.move heading_deg must be blank or numeric."
            issueSuggestion = "Clear heading_deg or enter a number in the range [0, 360)."
            Exit Function
        End If

        headingDeg = CDbl(hVal)
        If headingDeg < MOVE_HEADING_MIN_DEG Or headingDeg >= MOVE_HEADING_MAX_EXCLUSIVE_DEG Then
            issueCode = "COMMAND_ARG_OUT_OF_RANGE"
            issueMessage = "mobility.move heading_deg is outside the allowed range [0, 360)."
            issueSuggestion = "Clear heading_deg or enter a value greater than or equal to 0 and less than 360."
            Exit Function
        End If
    End If

    ValidateMobilityMoveRow = True
End Function

Private Sub SetMobilityMoveValidationError( _
    wsCmd As Worksheet, _
    ByVal r As Long, _
    ByVal issueCode As String, _
    ByVal issueMessage As String, _
    ByVal issueSuggestion As String)

    wsCmd.Cells(r, COL_ARGS_JSON).ClearContents
    wsCmd.Cells(r, COL_STATUS).value = "ERROR"
    wsCmd.Cells(r, COL_ISSUE_CODE).value = issueCode
    wsCmd.Cells(r, COL_MESSAGE).value = issueMessage
    wsCmd.Cells(r, COL_SUGGESTION).value = issueSuggestion
End Sub

Public Sub ExportScriptCsv()
    Dim wsCmd As Worksheet
    Dim wsExport As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)
    Set wsExport = ThisWorkbook.Worksheets(SHEET_EXPORT)

    ' Always rebuild args before exporting.
    Dim ok As Boolean
    ok = BuildArgsJsonSilent()
    If Not ok Then
        MsgBox "Export stopped because one or more enabled rows have Args JSON errors.", vbExclamation
        Exit Sub
    End If

    Dim outPath As String
    outPath = PickCsvOutputPath("experiment_script.csv")
    If outPath = "" Then
        Exit Sub
    End If

    Dim csvText As String
    csvText = BuildScriptCsvText(wsCmd, wsExport)

    WriteUtf8TextFile outPath, csvText

    MsgBox "Script CSV exported:" & vbCrLf & outPath, vbInformation
End Sub

Private Function BuildArgsJsonSilent() As Boolean
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    Dim r As Long
    Dim ok As Boolean
    ok = True

    Application.ScreenUpdating = False

    For r = 4 To lastRow
        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Then
            If IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) Then
                If Not BuildArgsJsonForRow(wsCmd, r) Then
                    ok = False
                End If
            End If
        End If
    Next r

    Application.ScreenUpdating = True

    BuildArgsJsonSilent = ok
End Function

Private Function BuildScriptCsvText(wsCmd As Worksheet, wsExport As Worksheet) As String
    Dim lastRow As Long
    lastRow = wsCmd.Cells(wsCmd.rows.Count, COL_CMD_ID).End(xlUp).Row

    wsExport.Cells.ClearContents
    wsExport.Range("A1:E1").value = Array("scanner", "t_offset_sec", "category", "action", "args_json")
    wsExport.Range("A1:E1").Font.Bold = True

    Dim csvText As String
    csvText = "scanner,t_offset_sec,category,action,args_json" & vbCrLf

    Dim r As Long
    Dim outRow As Long
    outRow = 2

    For r = 4 To lastRow
        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Then
            If IsTruthy(wsCmd.Cells(r, COL_ENABLED).value) Then
                Dim scanner As String
                Dim category As String
                Dim action As String
                Dim argsJson As String
                Dim tOffset As Long

                scanner = Trim$(CStr(wsCmd.Cells(r, COL_SCANNER).value))
                category = Trim$(CStr(wsCmd.Cells(r, COL_CATEGORY).value))
                action = Trim$(CStr(wsCmd.Cells(r, COL_ACTION).value))
                argsJson = Trim$(CStr(wsCmd.Cells(r, COL_ARGS_JSON).value))
                tOffset = TimeOffsetSeconds(wsCmd, r)

                wsExport.Cells(outRow, 1).value = scanner
                wsExport.Cells(outRow, 2).value = tOffset
                wsExport.Cells(outRow, 3).value = category
                wsExport.Cells(outRow, 4).value = action
                wsExport.Cells(outRow, 5).value = argsJson

                csvText = csvText & CsvEscape(scanner) & "," & _
                                    CStr(tOffset) & "," & _
                                    CsvEscape(category) & "," & _
                                    CsvEscape(action) & "," & _
                                    CsvEscape(argsJson) & vbCrLf

                outRow = outRow + 1
            End If
        End If
    Next r

    wsExport.Columns("A:E").AutoFit

    BuildScriptCsvText = csvText
End Function

Private Function TimeOffsetSeconds(wsCmd As Worksheet, ByVal r As Long) As Long
    Dim minuteVal As Double
    Dim secondVal As Double

    If IsNumeric(wsCmd.Cells(r, COL_MINUTE).value) Then
        minuteVal = CDbl(wsCmd.Cells(r, COL_MINUTE).value)
    Else
        minuteVal = 0
    End If

    If IsNumeric(wsCmd.Cells(r, COL_SECOND).value) Then
        secondVal = CDbl(wsCmd.Cells(r, COL_SECOND).value)
    Else
        secondVal = 0
    End If

    TimeOffsetSeconds = CLng(minuteVal * 60 + secondVal)
End Function

Private Function PickCsvOutputPath(ByVal defaultFileName As String) As String
    Dim folderPath As String

    With Application.FileDialog(4) ' msoFileDialogFolderPicker
        .Title = "Choose folder for exported script CSV"
        .AllowMultiSelect = False
        If .Show <> -1 Then
            PickCsvOutputPath = ""
            Exit Function
        End If
        folderPath = .SelectedItems(1)
    End With

    If Right$(folderPath, 1) = "\" Or Right$(folderPath, 1) = "/" Then
        PickCsvOutputPath = folderPath & defaultFileName
    Else
        PickCsvOutputPath = folderPath & Application.PathSeparator & defaultFileName
    End If
End Function

Private Sub WriteUtf8TextFile(ByVal filePath As String, ByVal textBody As String)
    ' Uses late-bound ADODB.Stream to write UTF-8 without requiring a VBA reference.
    Dim stream As Object
    Set stream = CreateObject("ADODB.Stream")

    With stream
        .Type = 2          ' adTypeText
        .Charset = "utf-8"
        .Open
        .WriteText textBody
        .SaveToFile filePath, 2   ' adSaveCreateOverWrite
        .Close
    End With
End Sub


' ============================================================
' Phase-5 initial poses export macros
' ============================================================

Public Sub ExportInitialPosesCsv()
    Dim wsPose As Worksheet
    Set wsPose = ThisWorkbook.Worksheets(SHEET_POSES)

    Dim wsPreview As Worksheet
    Set wsPreview = GetOrCreateSheet(SHEET_INITIAL_EXPORT)

    Dim csvText As String
    csvText = BuildInitialPosesCsvText(wsPose, wsPreview)

    If CountInitialPoseRows(wsPose) = 0 Then
        MsgBox "No enabled initial poses found. Export stopped.", vbExclamation
        Exit Sub
    End If

    Dim outPath As String
    outPath = PickCsvOutputPath("initial_poses.csv")
    If outPath = "" Then
        Exit Sub
    End If

    WriteUtf8TextFile outPath, csvText

    MsgBox "Initial poses CSV exported:" & vbCrLf & outPath, vbInformation
End Sub

Public Sub ExportAllCsv()
    ' Convenience macro for script writers.
    ' It exports experiment_script.csv first, then initial_poses.csv.
    ExportScriptCsv
    ExportInitialPosesCsv
End Sub


Private Sub ExportAllCsvToFolder(ByVal folderPath As String)
    EnsureFolderExists folderPath

    Dim wsCmd As Worksheet
    Dim wsExport As Worksheet
    Dim wsPose As Worksheet
    Dim wsPosePreview As Worksheet

    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)
    Set wsExport = ThisWorkbook.Worksheets(SHEET_EXPORT)
    Set wsPose = ThisWorkbook.Worksheets(SHEET_POSES)
    Set wsPosePreview = GetOrCreateSheet(SHEET_INITIAL_EXPORT)

    Dim ok As Boolean
    ok = BuildArgsJsonSilent()
    If Not ok Then
        Err.Raise vbObjectError + 6100, "ExportAllCsvToFolder", "One or more enabled command rows have Args JSON errors."
    End If

    Dim scriptCsv As String
    Dim poseCsv As String

    scriptCsv = BuildScriptCsvText(wsCmd, wsExport)
    poseCsv = BuildInitialPosesCsvText(wsPose, wsPosePreview)

    WriteUtf8TextFile JoinPath(folderPath, "experiment_script.csv"), scriptCsv
    WriteUtf8TextFile JoinPath(folderPath, "initial_poses.csv"), poseCsv
End Sub


Private Function BuildInitialPosesCsvText(wsPose As Worksheet, wsPreview As Worksheet) As String
    Dim lastRow As Long
    lastRow = wsPose.Cells(wsPose.rows.Count, 2).End(xlUp).Row

    wsPreview.Cells.ClearContents
    wsPreview.Range("A1:F1").value = Array("scanner", "intended_x_m", "intended_y_m", "intended_heading_deg", "position_tolerance_m", "heading_tolerance_deg")
    wsPreview.Range("A1:F1").Font.Bold = True

    Dim csvText As String
    csvText = "scanner,intended_x_m,intended_y_m,intended_heading_deg,position_tolerance_m,heading_tolerance_deg" & vbCrLf

    Dim r As Long
    Dim outRow As Long
    outRow = 2

    For r = 4 To lastRow
        If IsTruthy(wsPose.Cells(r, 1).value) Then
            Dim scanner As String
            Dim xM As Variant
            Dim yM As Variant
            Dim headingDeg As Variant

            scanner = Trim$(CStr(wsPose.Cells(r, 2).value))
            xM = wsPose.Cells(r, 3).value
            yM = wsPose.Cells(r, 4).value
            headingDeg = wsPose.Cells(r, 5).value

            If scanner <> "" And IsNumeric(xM) And IsNumeric(yM) And IsNumeric(headingDeg) Then
                wsPreview.Cells(outRow, 1).value = scanner
                wsPreview.Cells(outRow, 2).value = CDbl(xM)
                wsPreview.Cells(outRow, 3).value = CDbl(yM)
                wsPreview.Cells(outRow, 4).value = CDbl(headingDeg)
                wsPreview.Cells(outRow, 5).value = ""
                wsPreview.Cells(outRow, 6).value = ""

                csvText = csvText & CsvEscape(scanner) & "," & _
                                    JsonNumber(CDbl(xM)) & "," & _
                                    JsonNumber(CDbl(yM)) & "," & _
                                    JsonNumber(CDbl(headingDeg)) & "," & _
                                    "," & vbCrLf

                outRow = outRow + 1
            End If
        End If
    Next r

    wsPreview.Columns("A:F").AutoFit

    BuildInitialPosesCsvText = csvText
End Function

Private Function CountInitialPoseRows(wsPose As Worksheet) As Long
    Dim lastRow As Long
    lastRow = wsPose.Cells(wsPose.rows.Count, 2).End(xlUp).Row

    Dim r As Long
    Dim n As Long
    n = 0

    For r = 4 To lastRow
        If IsTruthy(wsPose.Cells(r, 1).value) Then
            If Trim$(CStr(wsPose.Cells(r, 2).value)) <> "" Then
                n = n + 1
            End If
        End If
    Next r

    CountInitialPoseRows = n
End Function

Private Function GetOrCreateSheet(ByVal sheetName As String) As Worksheet
    Dim ws As Worksheet

    On Error Resume Next
    Set ws = ThisWorkbook.Worksheets(sheetName)
    On Error GoTo 0

    If ws Is Nothing Then
        Set ws = ThisWorkbook.Worksheets.Add(After:=ThisWorkbook.Worksheets(ThisWorkbook.Worksheets.Count))
        ws.Name = sheetName
    End If

    Set GetOrCreateSheet = ws
End Function



' ============================================================
' Phase-6 CommonCheckers runner macro
' ============================================================

Public Sub RunCommonCheckers()
    If ThisWorkbook.path = "" Then
        MsgBox "Please save this workbook first. The macro needs the workbook folder for generated CSV and report files.", vbExclamation
        Exit Sub
    End If

    Dim generatedFolder As String
    generatedFolder = JoinPath(ThisWorkbook.path, "generated")

    Dim wsConfig As Worksheet
    Dim wsStatus As Worksheet
    Set wsConfig = ThisWorkbook.Worksheets(SHEET_PREFLIGHT_CONFIG)
    Set wsStatus = GetOrCreateSheet(SHEET_PREFLIGHT_STATUS)

    Dim pythonExe As String
    Dim nmsRoot As String
    Dim helperScript As String
    Dim siteDir As String
    Dim commonDir As String
    Dim scriptCsv As String
    Dim initialPosesCsv As String
    Dim reportJson As String
    Dim stdoutPath As String
    Dim feedbackCsv As String

    pythonExe = Trim$(CStr(wsConfig.Range("B4").value))
    nmsRoot = Trim$(CStr(wsConfig.Range("B5").value))
    helperScript = Trim$(CStr(wsConfig.Range("B6").value))

    If pythonExe = "" Then pythonExe = "python"
    If nmsRoot = "" Then nmsRoot = "D:\Data\_Action\_RunNMS"
    If helperScript = "" Then helperScript = JoinPath(ThisWorkbook.path, "autolab_xlsm_run_checker.py")

    siteDir = JoinPath(JoinPath(nmsRoot, "sitemap"), "DemoRoom")
    commonDir = JoinPath(JoinPath(nmsRoot, "sitemap"), "CommonCheckers")

    scriptCsv = JoinPath(generatedFolder, "experiment_script.csv")
    initialPosesCsv = JoinPath(generatedFolder, "initial_poses.csv")
    reportJson = JoinPath(generatedFolder, "validation_report.json")
    stdoutPath = JoinPath(generatedFolder, "checker_stdout.txt")
    feedbackCsv = JoinPath(generatedFolder, "validation_feedback.csv")

    If Not FileExists(helperScript) Then
        MsgBox "Python helper script not found:" & vbCrLf & helperScript & vbCrLf & vbCrLf & _
               "Put autolab_xlsm_run_checker.py in the same folder as this workbook, or update PreflightConfig B4.", vbExclamation
        Exit Sub
    End If

    On Error GoTo Failed

    Application.ScreenUpdating = False

    ExportAllCsvToFolder generatedFolder

    Dim innerCmd As String
    innerCmd = QuotePath(pythonExe) & " " & _
               QuotePath(helperScript) & " " & _
               "--script_csv " & QuotePath(scriptCsv) & " " & _
               "--initial_poses_csv " & QuotePath(initialPosesCsv) & " " & _
               "--site_dir " & QuotePath(siteDir) & " " & _
               "--common_dir " & QuotePath(commonDir) & " " & _
               "--report_json " & QuotePath(reportJson) & " " & _
               "--feedback_csv " & QuotePath(feedbackCsv) & _
               " > " & QuotePath(stdoutPath) & " 2>&1"

    Dim cmd As String
    cmd = Environ$("ComSpec") & " /S /C " & Chr$(34) & innerCmd & Chr$(34)

    ' Write status BEFORE launching Python so failures still leave the exact command visible.
    WritePreflightLaunchStatus wsStatus, generatedFolder, scriptCsv, initialPosesCsv, reportJson, stdoutPath, feedbackCsv, cmd

    Dim shell As Object
    Set shell = CreateObject("WScript.Shell")
    shell.CurrentDirectory = ThisWorkbook.path

    Dim exitCode As Long
    exitCode = shell.Run(cmd, 1, True)

    WritePreflightStatus wsStatus, exitCode, generatedFolder, scriptCsv, initialPosesCsv, reportJson, stdoutPath, feedbackCsv, cmd

    If FileExists(feedbackCsv) Then
        ImportValidationFeedbackFromFile feedbackCsv
    End If

    Application.ScreenUpdating = True

    If exitCode = 0 Then
        If FileExists(reportJson) Then
            MsgBox "CommonCheckers finished: PASS" & vbCrLf & reportJson, vbInformation
        Else
            MsgBox "CommonCheckers returned PASS but no validation_report.json was found." & vbCrLf & _
                   "Check:" & vbCrLf & stdoutPath, vbExclamation
        End If
    ElseIf exitCode = 1 Then
        If FileExists(reportJson) Then
            MsgBox "CommonCheckers finished: FAIL" & vbCrLf & _
                   "Report written to:" & vbCrLf & reportJson & vbCrLf & vbCrLf & _
                   "Phase-7 will import row feedback into the workbook.", vbExclamation
        Else
            MsgBox "CommonCheckers returned FAIL, but validation_report.json was not found." & vbCrLf & _
                   "Check:" & vbCrLf & stdoutPath, vbCritical
        End If
    Else
        MsgBox "CommonCheckers runner error. Exit code: " & exitCode & vbCrLf & _
               "Check:" & vbCrLf & stdoutPath, vbCritical
    End If

    Exit Sub

Failed:
    Application.ScreenUpdating = True
    WritePreflightException wsStatus, Err.Description
    MsgBox "RunCommonCheckers failed:" & vbCrLf & Err.Description, vbCritical
End Sub

Private Sub WritePreflightLaunchStatus(wsStatus As Worksheet, ByVal generatedFolder As String, _
                                        ByVal scriptCsv As String, ByVal initialPosesCsv As String, _
                                        ByVal reportJson As String, ByVal stdoutPath As String, _
                                        ByVal feedbackCsv As String, ByVal cmd As String)
    wsStatus.Cells.ClearContents
    wsStatus.Range("A1:B1").value = Array("Preflight Status", "")
    wsStatus.Range("A1:B1").Font.Bold = True

    WriteStatusRow wsStatus, 3, "Last run", Now
    WriteStatusRow wsStatus, 4, "Exit code", "RUNNING"
    WriteStatusRow wsStatus, 5, "Generated folder", generatedFolder
    WriteStatusRow wsStatus, 6, "Script CSV", scriptCsv
    WriteStatusRow wsStatus, 7, "Initial poses CSV", initialPosesCsv
    WriteStatusRow wsStatus, 8, "Report JSON", reportJson
    WriteStatusRow wsStatus, 9, "Report exists", IIf(FileExists(reportJson), "YES", "NO")
    WriteStatusRow wsStatus, 10, "Stdout log", stdoutPath
    WriteStatusRow wsStatus, 11, "Validation feedback CSV", feedbackCsv
    WriteStatusRow wsStatus, 12, "Result", "RUNNING"
    WriteStatusRow wsStatus, 13, "Next step", "Waiting for CommonCheckers process to finish."
    WriteStatusRow wsStatus, 14, "Command", cmd

    FormatStatusSheet wsStatus
End Sub

Private Sub WritePreflightStatus(wsStatus As Worksheet, ByVal exitCode As Long, ByVal generatedFolder As String, _
                                 ByVal scriptCsv As String, ByVal initialPosesCsv As String, _
                                 ByVal reportJson As String, ByVal stdoutPath As String, _
                                 ByVal feedbackCsv As String, ByVal cmd As String)
    wsStatus.Cells.ClearContents
    wsStatus.Range("A1:B1").value = Array("Preflight Status", "")
    wsStatus.Range("A1:B1").Font.Bold = True

    WriteStatusRow wsStatus, 3, "Last run", Now
    WriteStatusRow wsStatus, 4, "Exit code", exitCode
    WriteStatusRow wsStatus, 5, "Generated folder", generatedFolder
    WriteStatusRow wsStatus, 6, "Script CSV", scriptCsv
    WriteStatusRow wsStatus, 7, "Initial poses CSV", initialPosesCsv
    WriteStatusRow wsStatus, 8, "Report JSON", reportJson
    WriteStatusRow wsStatus, 9, "Report exists", IIf(FileExists(reportJson), "YES", "NO")
    WriteStatusRow wsStatus, 10, "Stdout log", stdoutPath
    WriteStatusRow wsStatus, 11, "Validation feedback CSV", feedbackCsv
    WriteStatusRow wsStatus, 12, "Result", IIf(exitCode = 0, "PASS", IIf(exitCode = 1, "FAIL", "RUNNER ERROR"))
    WriteStatusRow wsStatus, 13, "Next step", "ValidationReport and CommandSheet feedback were updated if validation_feedback.csv exists."
    WriteStatusRow wsStatus, 14, "Command", cmd

    FormatStatusSheet wsStatus
End Sub

Private Sub WritePreflightException(wsStatus As Worksheet, ByVal message As String)
    wsStatus.Cells.ClearContents
    wsStatus.Range("A1:B1").value = Array("Preflight Status", "")
    wsStatus.Range("A1:B1").Font.Bold = True

    WriteStatusRow wsStatus, 3, "Last run", Now
    WriteStatusRow wsStatus, 4, "Result", "VBA ERROR"
    WriteStatusRow wsStatus, 5, "Message", message

    FormatStatusSheet wsStatus
End Sub

Private Sub WriteStatusRow(wsStatus As Worksheet, ByVal rowNum As Long, ByVal key As String, ByVal value As Variant)
    wsStatus.Cells(rowNum, 1).value = key
    wsStatus.Cells(rowNum, 2).value = value
End Sub

Private Sub FormatStatusSheet(wsStatus As Worksheet)
    wsStatus.Columns("A:B").AutoFit
    wsStatus.Range("B3:B25").WrapText = True
    wsStatus.Range("A3:B25").Borders.LineStyle = xlContinuous
End Sub

Private Function QuotePath(ByVal s As String) As String
    QuotePath = Chr$(34) & s & Chr$(34)
End Function

Private Function FileExists(ByVal path As String) As Boolean
    FileExists = (Dir(path, vbNormal) <> "")
End Function

Private Sub EnsureFolderExists(ByVal folderPath As String)
    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")

    If Not fso.FolderExists(folderPath) Then
        fso.CreateFolder folderPath
    End If
End Sub

Private Function JoinPath(ByVal leftPart As String, ByVal rightPart As String) As String
    If Right$(leftPart, 1) = "\" Or Right$(leftPart, 1) = "/" Then
        JoinPath = leftPart & rightPart
    Else
        JoinPath = leftPart & Application.PathSeparator & rightPart
    End If
End Function




Private Function LastUsedRow(ws As Worksheet) As Long
    ' Return the last used row based on column A, which holds CmdRowID in CommandSheet.
    LastUsedRow = ws.Cells(ws.rows.Count, 1).End(xlUp).Row
End Function

Private Function IsValueRow(wsCmd As Worksheet, ByVal rowNum As Long) As Boolean
    IsValueRow = (LCase$(Trim$(CStr(wsCmd.Cells(rowNum, COL_LINE_TYPE).value))) = "value")
End Function

Private Function IsEnabledValue(ByVal v As Variant) As Boolean
    IsEnabledValue = IsTruthy(v)
End Function


' ============================================================
' Phase-7 validation report import macros
' ============================================================

Public Sub ImportValidationFeedback()
    If ThisWorkbook.path = "" Then
        MsgBox "Please save this workbook first.", vbExclamation
        Exit Sub
    End If

    Dim feedbackCsv As String
    feedbackCsv = JoinPath(JoinPath(ThisWorkbook.path, "generated"), "validation_feedback.csv")

    If Not FileExists(feedbackCsv) Then
        MsgBox "validation_feedback.csv not found:" & vbCrLf & feedbackCsv & vbCrLf & _
               "Run RunCommonCheckers first.", vbExclamation
        Exit Sub
    End If

    ImportValidationFeedbackFromFile feedbackCsv
    MsgBox "Validation feedback imported.", vbInformation
End Sub

Public Sub ClearValidationFeedback()
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Dim lastRow As Long
    lastRow = LastUsedRow(wsCmd)

    Dim r As Long
    For r = FIRST_DATA_ROW To lastRow
        If IsValueRow(wsCmd, r) Then
            If IsEnabledValue(wsCmd.Cells(r, COL_ENABLED).value) Then
                wsCmd.Cells(r, COL_STATUS).value = "NOT CHECKED"
            Else
                wsCmd.Cells(r, COL_STATUS).value = "DISABLED"
            End If
            wsCmd.Cells(r, COL_ISSUE_CODE).value = ""
            wsCmd.Cells(r, COL_MESSAGE).value = ""
            wsCmd.Cells(r, COL_SUGGESTION).value = ""
        End If
    Next r
End Sub

Private Sub ImportValidationFeedbackFromFile(ByVal feedbackCsvPath As String)
    Dim csvText As String
    csvText = ReadTextFileUtf8(feedbackCsvPath)

    Dim rows As Collection
    Set rows = ParseCsvText(csvText)

    Dim wsReport As Worksheet
    Dim wsCmd As Worksheet
    Set wsReport = GetOrCreateSheet(SHEET_VALIDATION_REPORT)
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    wsReport.Cells.ClearContents

    If rows.Count = 0 Then
        wsReport.Range("A1").value = "No validation feedback rows found."
        Exit Sub
    End If

    WriteCsvRowsToSheet wsReport, rows
    ApplyValidationFeedbackToCommandSheet wsCmd, rows

    wsReport.Columns("A:K").AutoFit
    wsReport.Range("A1:K1").Font.Bold = True
    wsReport.Range("A1:K1").Interior.Color = RGB(217, 234, 247)
    wsReport.Range("A:K").WrapText = True
End Sub

Private Sub ApplyValidationFeedbackToCommandSheet(wsCmd As Worksheet, ByVal rows As Collection)
    ClearValidationFeedback
    MarkCheckedRowsWithoutIssuesOk wsCmd

    Dim i As Long
    For i = 2 To rows.Count
        Dim fields As Variant
        fields = rows(i)

        If UBound(fields) >= 6 Then
            Dim cmdRowId As Long
            Dim statusText As String
            Dim issueCode As String
            Dim messageText As String
            Dim suggestionText As String
            Dim excelRow As Long

            cmdRowId = CLng(Val(CStr(fields(0))))
            statusText = CStr(fields(2))
            issueCode = CStr(fields(4))
            messageText = CStr(fields(5))
            suggestionText = CStr(fields(6))

            excelRow = FindCommandValueRow(wsCmd, cmdRowId)
            If excelRow > 0 Then
                wsCmd.Cells(excelRow, COL_STATUS).value = statusText
                wsCmd.Cells(excelRow, COL_ISSUE_CODE).value = issueCode
                wsCmd.Cells(excelRow, COL_MESSAGE).value = messageText
                wsCmd.Cells(excelRow, COL_SUGGESTION).value = suggestionText
            End If
        End If
    Next i

    ColorFeedbackRows wsCmd
End Sub


Private Sub MarkCheckedRowsWithoutIssuesOk(wsCmd As Worksheet)
    Dim lastRow As Long
    lastRow = LastUsedRow(wsCmd)

    Dim r As Long
    For r = FIRST_DATA_ROW To lastRow
        If IsValueRow(wsCmd, r) Then
            If IsEnabledValue(wsCmd.Cells(r, COL_ENABLED).value) Then
                wsCmd.Cells(r, COL_STATUS).value = "OK"
            Else
                wsCmd.Cells(r, COL_STATUS).value = "DISABLED"
            End If
            wsCmd.Cells(r, COL_ISSUE_CODE).value = ""
            wsCmd.Cells(r, COL_MESSAGE).value = ""
            wsCmd.Cells(r, COL_SUGGESTION).value = ""
        End If
    Next r
End Sub


Private Function FindCommandValueRow(wsCmd As Worksheet, ByVal cmdRowId As Long) As Long
    Dim lastRow As Long
    lastRow = LastUsedRow(wsCmd)

    Dim r As Long
    For r = FIRST_DATA_ROW To lastRow
        If IsValueRow(wsCmd, r) Then
            If CLng(Val(CStr(wsCmd.Cells(r, COL_CMD_ID).value))) = cmdRowId Then
                FindCommandValueRow = r
                Exit Function
            End If
        End If
    Next r

    FindCommandValueRow = 0
End Function

Private Sub ColorFeedbackRows(wsCmd As Worksheet)
    Dim lastRow As Long
    lastRow = LastUsedRow(wsCmd)

    Dim r As Long
    For r = FIRST_DATA_ROW To lastRow
        If IsValueRow(wsCmd, r) Then
            Dim statusText As String
            statusText = UCase$(Trim$(CStr(wsCmd.Cells(r, COL_STATUS).value)))

            Select Case statusText
                Case "OK"
                    wsCmd.Range(wsCmd.Cells(r, COL_STATUS), wsCmd.Cells(r, COL_SUGGESTION)).Interior.Color = RGB(226, 239, 218)
                Case "ERROR"
                    wsCmd.Range(wsCmd.Cells(r, COL_STATUS), wsCmd.Cells(r, COL_SUGGESTION)).Interior.Color = RGB(252, 228, 214)
                Case "WARNING"
                    wsCmd.Range(wsCmd.Cells(r, COL_STATUS), wsCmd.Cells(r, COL_SUGGESTION)).Interior.Color = RGB(255, 242, 204)
                Case "DISABLED"
                    wsCmd.Range(wsCmd.Cells(r, COL_STATUS), wsCmd.Cells(r, COL_SUGGESTION)).Interior.Color = RGB(217, 217, 217)
                Case Else
                    wsCmd.Range(wsCmd.Cells(r, COL_STATUS), wsCmd.Cells(r, COL_SUGGESTION)).Interior.ColorIndex = xlNone
            End Select
        End If
    Next r
End Sub

Private Sub WriteCsvRowsToSheet(ws As Worksheet, ByVal rows As Collection)
    Dim r As Long
    Dim c As Long

    For r = 1 To rows.Count
        Dim fields As Variant
        fields = rows(r)
        For c = LBound(fields) To UBound(fields)
            ws.Cells(r, c + 1).value = fields(c)
        Next c
    Next r
End Sub

Private Function ParseCsvText(ByVal csvText As String) As Collection
    Dim rows As New Collection
    Dim rowFields As Collection
    Set rowFields = New Collection

    Dim field As String
    Dim inQuotes As Boolean
    Dim i As Long
    Dim ch As String
    Dim nextCh As String

    csvText = Replace(csvText, vbCrLf, vbLf)
    csvText = Replace(csvText, vbCr, vbLf)

    For i = 1 To Len(csvText)
        ch = Mid$(csvText, i, 1)

        If inQuotes Then
            If ch = """" Then
                If i < Len(csvText) Then
                    nextCh = Mid$(csvText, i + 1, 1)
                    If nextCh = """" Then
                        field = field & """"
                        i = i + 1
                    Else
                        inQuotes = False
                    End If
                Else
                    inQuotes = False
                End If
            Else
                field = field & ch
            End If
        Else
            Select Case ch
                Case """"
                    inQuotes = True
                Case ","
                    rowFields.Add field
                    field = ""
                Case vbLf
                    rowFields.Add field
                    rows.Add CollectionToArray(rowFields)
                    Set rowFields = New Collection
                    field = ""
                Case Else
                    field = field & ch
            End Select
        End If
    Next i

    If Len(field) > 0 Or rowFields.Count > 0 Then
        rowFields.Add field
        rows.Add CollectionToArray(rowFields)
    End If

    Set ParseCsvText = rows
End Function

Private Function CollectionToArray(ByVal col As Collection) As Variant
    Dim arr() As String
    Dim i As Long
    ReDim arr(0 To col.Count - 1)

    For i = 1 To col.Count
        arr(i - 1) = CStr(col(i))
    Next i

    CollectionToArray = arr
End Function

Private Function ReadTextFileUtf8(ByVal filePath As String) As String
    Dim stream As Object
    Set stream = CreateObject("ADODB.Stream")

    stream.Type = 2
    stream.Charset = "utf-8"
    stream.Open
    stream.LoadFromFile filePath
    ReadTextFileUtf8 = stream.ReadText
    stream.Close
End Function



' ============================================================
' Phase-8a GUI hardening macros
' ============================================================

Public Sub ApplyCommandGuiRules()
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Application.ScreenUpdating = False
    Application.EnableEvents = False

    wsCmd.Unprotect

    ApplyCommandDropdowns wsCmd
    ApplyCommandSheetBaseProtection wsCmd

    Dim r As Long
    For r = FIRST_DATA_ROW To COMMAND_GUI_MAX_ROW
        If IsValueRow(wsCmd, r) Then
            ApplyCommandLayoutToValueRow wsCmd, r
        End If
    Next r

    wsCmd.Protect UserInterfaceOnly:=True, AllowFormattingCells:=True, AllowFormattingColumns:=True, AllowFormattingRows:=True

    Application.EnableEvents = True
    Application.ScreenUpdating = True

    MsgBox "Command GUI rules applied. CommandID cells now use strict dropdown validation and parameter cells are refreshed.", vbInformation
End Sub

Public Sub RefreshSelectedCommandLayout()
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    If ActiveSheet.Name <> SHEET_COMMAND Then
        MsgBox "Please select a Key or Value row in CommandSheet first.", vbExclamation
        Exit Sub
    End If

    Dim valueRow As Long
    valueRow = ResolveSelectedCommandValueRow(wsCmd)
    If valueRow = 0 Then
        MsgBox "Please select a Key or Value row in CommandSheet first.", vbExclamation
        Exit Sub
    End If

    Application.EnableEvents = False
    wsCmd.Unprotect
    ApplyCommandLayoutToValueRow wsCmd, valueRow
    wsCmd.Protect UserInterfaceOnly:=True, AllowFormattingCells:=True, AllowFormattingColumns:=True, AllowFormattingRows:=True
    Application.EnableEvents = True

    MsgBox "Command layout refreshed for CmdRowID " & wsCmd.Cells(valueRow, COL_CMD_ID).value & ".", vbInformation
End Sub

Public Sub AutoLabGui_HandleCommandIdChange(ByVal Target As Range)
    Dim wsCmd As Worksheet
    Set wsCmd = ThisWorkbook.Worksheets(SHEET_COMMAND)

    Dim cell As Range
    For Each cell In Target.Cells
        If cell.Column = COL_COMMAND_ID Then
            Dim valueRow As Long
            valueRow = ResolveChangedCommandValueRow(wsCmd, cell.Row)
            If valueRow > 0 Then
                wsCmd.Unprotect
                ApplyCommandLayoutToValueRow wsCmd, valueRow
                wsCmd.Protect UserInterfaceOnly:=True, AllowFormattingCells:=True, AllowFormattingColumns:=True, AllowFormattingRows:=True
            End If
        End If
    Next cell
End Sub

Private Function ResolveChangedCommandValueRow(wsCmd As Worksheet, ByVal rowNum As Long) As Long
    Dim lineType As String
    lineType = LCase$(Trim$(CStr(wsCmd.Cells(rowNum, COL_LINE_TYPE).value)))

    If lineType = "value" Then
        ResolveChangedCommandValueRow = rowNum
    ElseIf lineType = "key" Then
        If LCase$(Trim$(CStr(wsCmd.Cells(rowNum + 1, COL_LINE_TYPE).value))) = "value" Then
            ResolveChangedCommandValueRow = rowNum + 1
        End If
    Else
        ResolveChangedCommandValueRow = 0
    End If
End Function

Private Sub ApplyCommandDropdowns(wsCmd As Worksheet)
    Dim r As Long
    For r = FIRST_DATA_ROW To COMMAND_GUI_MAX_ROW
        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Or wsCmd.Cells(r, COL_LINE_TYPE).value = "" Then
            With wsCmd.Cells(r, COL_COMMAND_ID).Validation
                .Delete
                .Add Type:=xlValidateList, AlertStyle:=xlValidAlertStop, Operator:=xlBetween, Formula1:=CMD_LIST_CSV
                .IgnoreBlank = False
                .InCellDropdown = True
                .ShowError = True
                .ErrorTitle = "Invalid CommandID"
                .ErrorMessage = "Choose a CommandID from the dropdown list. Manual values outside the supported command list are not allowed."
                .ShowInput = True
                .InputTitle = "CommandID"
                .InputMessage = "Choose one of the supported public robot script commands."
            End With
        End If
    Next r
End Sub

Private Sub ApplyCommandSheetBaseProtection(wsCmd As Worksheet)
    wsCmd.Range("A:U").Locked = True

    Dim r As Long
    For r = FIRST_DATA_ROW To COMMAND_GUI_MAX_ROW
        If LCase$(Trim$(CStr(wsCmd.Cells(r, COL_LINE_TYPE).value))) = "value" Or wsCmd.Cells(r, COL_LINE_TYPE).value = "" Then
            wsCmd.Cells(r, COL_ENABLED).Locked = False
            wsCmd.Cells(r, COL_MINUTE).Locked = False
            wsCmd.Cells(r, COL_SECOND).Locked = False
            wsCmd.Cells(r, COL_SCANNER).Locked = False
            wsCmd.Cells(r, COL_COMMAND_ID).Locked = False
        End If
    Next r
End Sub

Private Sub ApplyCommandLayoutToValueRow(wsCmd As Worksheet, ByVal valueRow As Long)
    Dim keyRow As Long
    keyRow = valueRow - 1

    Dim commandId As String
    commandId = Trim$(CStr(wsCmd.Cells(valueRow, COL_COMMAND_ID).value))

    ClearParamKeyLabels wsCmd, keyRow
    ClearParamEditStyle wsCmd, valueRow

    If commandId = "" Then
        wsCmd.Cells(valueRow, COL_STATUS).value = "NOT CHECKED"
        Exit Sub
    End If

    If Not IsSupportedCommandId(commandId) Then
        wsCmd.Cells(valueRow, COL_STATUS).value = "ERROR"
        wsCmd.Cells(valueRow, COL_ISSUE_CODE).value = "UNKNOWN_ACTION"
        wsCmd.Cells(valueRow, COL_MESSAGE).value = "Unsupported CommandID for current robot-script template: " & commandId
        wsCmd.Cells(valueRow, COL_SUGGESTION).value = "Choose a CommandID from the dropdown list."
        wsCmd.Cells(valueRow, COL_COMMAND_ID).Interior.Color = RGB(252, 228, 214)
        Exit Sub
    End If

    wsCmd.Cells(valueRow, COL_COMMAND_ID).Interior.Color = RGB(255, 255, 255)
    wsCmd.Cells(valueRow, COL_CATEGORY).value = CategoryForCommandId(commandId)
    wsCmd.Cells(valueRow, COL_ACTION).value = commandId

    Select Case commandId
        Case "mobility.move"
            wsCmd.Cells(keyRow, COL_PARAM1).value = "x_m"
            wsCmd.Cells(keyRow, COL_PARAM2).value = "y_m"
            wsCmd.Cells(keyRow, COL_PARAM3).value = "heading_deg"
            UnlockParamCell wsCmd.Cells(valueRow, COL_PARAM1)
            UnlockParamCell wsCmd.Cells(valueRow, COL_PARAM2)
            UnlockParamCell wsCmd.Cells(valueRow, COL_PARAM3)
            wsCmd.Cells(valueRow, COL_PARAM3).Interior.Color = RGB(255, 242, 204)

        Case "mobility.report.location", "mobility.in2out", "mobility.out2in", _
             "scan.start", "scan.stop", "scan.once"
            ' These commands use fixed args_json = {} and no editable params in Phase-10a.
            wsCmd.Range(wsCmd.Cells(valueRow, COL_PARAM1), wsCmd.Cells(valueRow, COL_PARAM6)).ClearContents
    End Select

    wsCmd.Cells(valueRow, COL_STATUS).value = IIf(IsEnabledValue(wsCmd.Cells(valueRow, COL_ENABLED).value), "NOT CHECKED", "DISABLED")
    wsCmd.Cells(valueRow, COL_ISSUE_CODE).value = ""
    wsCmd.Cells(valueRow, COL_MESSAGE).value = ""
    wsCmd.Cells(valueRow, COL_SUGGESTION).value = ""
End Sub

Private Sub ClearParamKeyLabels(wsCmd As Worksheet, ByVal keyRow As Long)
    If keyRow < FIRST_DATA_ROW Then Exit Sub
    wsCmd.Range(wsCmd.Cells(keyRow, COL_PARAM1), wsCmd.Cells(keyRow, COL_PARAM6)).ClearContents
    wsCmd.Range(wsCmd.Cells(keyRow, COL_PARAM1), wsCmd.Cells(keyRow, COL_PARAM6)).Interior.Color = RGB(242, 242, 242)
End Sub

Private Sub ClearParamEditStyle(wsCmd As Worksheet, ByVal valueRow As Long)
    With wsCmd.Range(wsCmd.Cells(valueRow, COL_PARAM1), wsCmd.Cells(valueRow, COL_PARAM6))
        .Locked = True
        .Interior.Color = RGB(242, 242, 242)
        .Font.Color = RGB(128, 128, 128)
        .ClearComments
    End With
End Sub

Private Sub UnlockParamCell(ByVal cell As Range)
    cell.Locked = False
    cell.Interior.Color = RGB(255, 255, 255)
    cell.Font.Color = RGB(0, 0, 0)
End Sub

Private Function IsSupportedCommandId(ByVal commandId As String) As Boolean
    Select Case Trim$(commandId)
        Case "mobility.report.location", "mobility.move", "mobility.in2out", "mobility.out2in", _
             "scan.start", "scan.stop", "scan.once"
            IsSupportedCommandId = True
        Case Else
            IsSupportedCommandId = False
    End Select
End Function

Private Function CategoryForCommandId(ByVal commandId As String) As String
    Dim s As String
    s = Trim$(commandId)

    If Left$(s, 9) = "mobility." Then
        CategoryForCommandId = "mobility"
    ElseIf Left$(s, 5) = "scan." Then
        CategoryForCommandId = "scan"
    Else
        CategoryForCommandId = ""
    End If
End Function




Private Function IsMacroCommand(ByVal action As String) As Boolean
    IsMacroCommand = (Trim$(action) = "mobility.in2out" Or Trim$(action) = "mobility.out2in")
End Function

Private Function GetMacroSegment(ByVal action As String, ByRef x0 As Double, ByRef y0 As Double, ByRef x1 As Double, ByRef y1 As Double) As Boolean
    Dim headingDeg As Double
    Dim distanceM As Double

    If Not GetMacroNumber(action, "start_x_m", x0) Then Exit Function
    If Not GetMacroNumber(action, "start_y_m", y0) Then Exit Function
    If Not GetMacroNumber(action, "target_heading_deg", headingDeg) Then Exit Function
    If Not GetMacroNumber(action, "distance_m", distanceM) Then Exit Function

    x1 = x0 + distanceM * Cos(DegToRad(headingDeg))
    y1 = y0 + distanceM * Sin(DegToRad(headingDeg))
    GetMacroSegment = True
End Function

Private Function GetMacroHeading(ByVal action As String, ByRef headingDeg As Double) As Boolean
    GetMacroHeading = GetMacroNumber(action, "target_heading_deg", headingDeg)
End Function

Private Function GetMacroNumber(ByVal action As String, ByVal keyName As String, ByRef outValue As Double) As Boolean
    Dim jsonText As String
    jsonText = LoadMacroPolicyText()

    If jsonText <> "" Then
        Dim macroBlock As String
        macroBlock = ExtractJsonObjectBlock(jsonText, """" & action & """")
        If macroBlock <> "" Then
            Dim textValue As String
            textValue = ExtractJsonNumber(macroBlock, """" & keyName & """")
            If textValue <> "" And IsNumeric(textValue) Then
                outValue = CDbl(textValue)
                GetMacroNumber = True
                Exit Function
            End If
        End If
    End If

    ' Last-resort fallback keeps map preview usable if the policy file cannot be read.
    ' CommonCheckers remains the rule authority.
    Select Case action
        Case "mobility.in2out"
            Select Case keyName
                Case "start_x_m": outValue = 9#
                Case "start_y_m": outValue = 4.35
                Case "target_heading_deg": outValue = 90#
                Case "distance_m": outValue = 2#
                Case Else: Exit Function
            End Select
            GetMacroNumber = True

        Case "mobility.out2in"
            Select Case keyName
                Case "start_x_m": outValue = 9#
                Case "start_y_m": outValue = 6.15
                Case "target_heading_deg": outValue = 270#
                Case "distance_m": outValue = 2#
                Case Else: Exit Function
            End Select
            GetMacroNumber = True
    End Select
End Function


Private Function GetMacroPolicyStart(ByVal action As String, ByRef startX As Double, ByRef startY As Double) As Boolean
    If Not GetMacroNumber(action, "start_x_m", startX) Then Exit Function
    If Not GetMacroNumber(action, "start_y_m", startY) Then Exit Function
    GetMacroPolicyStart = True
End Function

Private Function GetMacroEndpointFromCurrent(ByVal action As String, ByVal currentX As Double, ByVal currentY As Double, _
                                             ByRef endX As Double, ByRef endY As Double) As Boolean
    Dim headingDeg As Double
    Dim distanceM As Double

    If Not GetMacroNumber(action, "target_heading_deg", headingDeg) Then Exit Function
    If Not GetMacroNumber(action, "distance_m", distanceM) Then Exit Function

    endX = currentX + distanceM * Cos(DegToRad(headingDeg))
    endY = currentY + distanceM * Sin(DegToRad(headingDeg))
    GetMacroEndpointFromCurrent = True
End Function


Private Function LoadMacroPolicyText() As String
    Dim path1 As String
    Dim path2 As String
    Dim nmsRoot As String

    path1 = JoinPath(JoinPath(ThisWorkbook.path, "config"), "macro_policy.json")
    If FileExists(path1) Then
        LoadMacroPolicyText = ReadTextFileUtf8(path1)
        Exit Function
    End If

    On Error Resume Next
    nmsRoot = Trim$(CStr(ThisWorkbook.Worksheets(SHEET_PREFLIGHT_CONFIG).Range("B5").value))
    On Error GoTo 0

    If nmsRoot <> "" Then
        path2 = JoinPath(JoinPath(JoinPath(JoinPath(nmsRoot, "sitemap"), "DemoRoom"), "script_authoring"), "config")
        path2 = JoinPath(path2, "macro_policy.json")
        If FileExists(path2) Then
            LoadMacroPolicyText = ReadTextFileUtf8(path2)
            Exit Function
        End If
    End If

    LoadMacroPolicyText = ""
End Function

Private Function ExtractJsonObjectBlock(ByVal jsonText As String, ByVal quotedName As String) As String
    Dim p As Long
    p = InStr(1, jsonText, quotedName, vbTextCompare)
    If p <= 0 Then Exit Function

    Dim braceStart As Long
    braceStart = InStr(p, jsonText, "{")
    If braceStart <= 0 Then Exit Function

    Dim depth As Long
    Dim i As Long
    Dim ch As String

    For i = braceStart To Len(jsonText)
        ch = Mid$(jsonText, i, 1)
        If ch = "{" Then
            depth = depth + 1
        ElseIf ch = "}" Then
            depth = depth - 1
            If depth = 0 Then
                ExtractJsonObjectBlock = Mid$(jsonText, braceStart, i - braceStart + 1)
                Exit Function
            End If
        End If
    Next i
End Function

Private Function ExtractJsonNumber(ByVal jsonText As String, ByVal quotedKey As String) As String
    Dim p As Long
    p = InStr(1, jsonText, quotedKey, vbTextCompare)
    If p <= 0 Then Exit Function

    Dim colonPos As Long
    colonPos = InStr(p + Len(quotedKey), jsonText, ":")
    If colonPos <= 0 Then Exit Function

    Dim i As Long
    Dim ch As String
    Dim out As String

    For i = colonPos + 1 To Len(jsonText)
        ch = Mid$(jsonText, i, 1)
        If ch = " " Or ch = vbTab Or ch = vbCr Or ch = vbLf Then
            If out <> "" Then Exit For
        ElseIf (ch >= "0" And ch <= "9") Or ch = "-" Or ch = "+" Or ch = "." Or ch = "e" Or ch = "E" Then
            out = out & ch
        Else
            Exit For
        End If
    Next i

    ExtractJsonNumber = out
End Function

Private Function DegToRad(ByVal degrees As Double) As Double
    DegToRad = degrees * 3.14159265358979 / 180#
End Function


' ============================================================
' Shared helpers
' ============================================================

Private Sub XYToGrid(ByVal xM As Double, ByVal yM As Double, ByRef matrixRow As Long, ByRef matrixCol As Long)
    ' Match CommonCheckers/static_safety_core.py world_to_grid_unclamped:
    '   row = floor(y / resolution)
    '   col = floor(x / resolution)
    '
    ' In VBA, values such as 9.0 / 0.1 can evaluate as 89.999999999...
    ' because 0.1 is not exactly representable in binary floating point.
    ' Without a tiny positive tolerance, an exact grid boundary can be drawn
    ' into the cell on the left/down side. CommonCheckers treats x=9.0 as
    ' col=90, so the map should do the same.
    Const GRID_EPS As Double = 0.000000001

    matrixCol = Int((xM / CELL_M) + GRID_EPS)
    matrixRow = Int((yM / CELL_M) + GRID_EPS)
End Sub

Private Function GridCell(wsMap As Worksheet, ByVal matrixRow As Long, ByVal matrixCol As Long) As Range
    Dim displayY As Long
    Dim excelRow As Long
    Dim excelCol As Long

    displayY = MAP_ROWS - 1 - matrixRow
    excelRow = MAP_TOP + displayY
    excelCol = MAP_LEFT + matrixCol

    Set GridCell = wsMap.Cells(excelRow, excelCol)
End Function

Private Function InGrid(ByVal matrixRow As Long, ByVal matrixCol As Long) As Boolean
    InGrid = (matrixRow >= 0 And matrixRow < MAP_ROWS And matrixCol >= 0 And matrixCol < MAP_COLS)
End Function

Private Function IsStaticRestrictedCell(cell As Range) As Boolean
    IsStaticRestrictedCell = (cell.Interior.Color = RGB(255, 0, 0))
End Function

Private Function IsBumpGuardCell(cell As Range) As Boolean
    IsBumpGuardCell = (cell.Interior.Color = RGB(255, 217, 102))
End Function

Private Function IsDynamicZoneCell(cell As Range) As Boolean
    IsDynamicZoneCell = (cell.Interior.Color = RGB(157, 195, 230))
End Function


Private Function IsBlankCellValue(ByVal v As Variant) As Boolean
    If IsEmpty(v) Then
        IsBlankCellValue = True
    ElseIf IsNull(v) Then
        IsBlankCellValue = True
    Else
        IsBlankCellValue = (Trim$(CStr(v)) = "")
    End If
End Function

Private Function IsTruthy(v As Variant) As Boolean
    Dim s As String
    s = LCase$(Trim$(CStr(v)))
    IsTruthy = (s = "true" Or s = "yes" Or s = "1")
End Function

Private Function RobotShortLabel(ByVal scanner As String) As String
    Select Case scanner
        Case "twin-scout-alpha"
            RobotShortLabel = "A"
        Case "twin-scout-bravo"
            RobotShortLabel = "B"
        Case "twin-scout-charlie"
            RobotShortLabel = "C"
        Case "twin-scout-delta"
            RobotShortLabel = "D"
        Case Else
            RobotShortLabel = "R"
    End Select
End Function

Private Function JsonNumber(ByVal d As Double) As String
    Dim s As String
    s = Trim$(CStr(d))
    s = Replace(s, ",", ".")
    JsonNumber = s
End Function

Private Function CsvEscape(ByVal s As String) As String
    CsvEscape = """" & Replace(s, """", """""") & """"
End Function

Private Sub ClearFeedback(wsCmd As Worksheet, ByVal r As Long)
    wsCmd.Cells(r, COL_ISSUE_CODE).ClearContents
    wsCmd.Cells(r, COL_MESSAGE).ClearContents
    wsCmd.Cells(r, COL_SUGGESTION).ClearContents
End Sub




