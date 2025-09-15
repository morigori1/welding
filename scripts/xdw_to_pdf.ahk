; AutoHotkey v1 script
; Usage: AutoHotkey.exe scripts\\xdw_to_pdf.ahk "INPUT_FILE" "OUTPUT_PDF"
; Converts DocuWorks XDW/XBD to PDF by printing to the current default PDF printer
; (e.g., Microsoft Print to PDF). Ensure the printer prompts for a path, or set
; DocuWorks PDF to auto-save.

#NoEnv
#SingleInstance Force
SetBatchLines, -1
SetTitleMatchMode, 2
DetectHiddenWindows, On

if (A_Args.MaxIndex() < 2) {
    MsgBox, 48, Usage, xdw_to_pdf.ahk INPUT_FILE OUTPUT_PDF
    ExitApp, 2
}

input := A_Args[1]
output := A_Args[2]
; sanitize
StringReplace, input, input, /, \, All
StringReplace, output, output, /, \, All
input := Trim(input)
output := Trim(output)

; Launch with the default associated app (DocuWorks Viewer Light)
attr := FileExist(input)
if (!attr) {
  StringReplace, input, input, ", , All
}
attr := FileExist(input)
if (!attr) {
  ExitApp, 10
}
if InStr(attr, "D") {
  ExitApp, 12
}
; Always quote path
Run, "%input%", , , pid
if (!pid) {
    MsgBox, 16, Error, Failed to launch: %input%
    ExitApp, 3
}

; Wait for the viewer window
WinWait, ahk_pid %pid%, , 10000
if (ErrorLevel) {
    ; Fallback small wait
    Sleep, 500
}

; Bring to front and open Print dialog
WinActivate, ahk_pid %pid%
Sleep, 200
Send, ^p

; Wait for Print dialog and accept default printer
WinWaitActive, ahk_class #32770, , 10000
if (ErrorLevel) {
    ; Try again
    Sleep, 500
    Send, ^p
    WinWaitActive, ahk_class #32770, , 8000
}
; Confirm print (OK)
Send, {Enter}

; Wait for Save As dialog (Microsoft Print to PDF)
WinWaitActive, ahk_class #32770, , 15000
if (ErrorLevel) {
    ; If DocuWorks PDF auto-saves, file may be created without dialog
    ; Wait briefly for file to appear
    tries := 0
    Loop, 50 {
        FileGetAttrib, attrs, %output%
        if (ErrorLevel = 0) {
            success := 1
            break
        }
        Sleep, 200
    }
    goto CloseAndExit
}

; Type the path into the File name box (Edit1) and save
ControlFocus, Edit1, A
ControlSetText, Edit1, %output%, A
Sleep, 200
Send, {Enter}

; Handle overwrite confirmation if appears
WinWait, ahk_class #32770, , 2000
if (!ErrorLevel) {
    WinGetTitle, ttl, A
    if (InStr(ttl, "Overwrite") || InStr(ttl, "上書き") || InStr(ttl, "確認") || InStr(ttl, "Confirm")) {
        Send, !y
    }
}

; Wait for file creation
success := 0
Loop, 120 {
    if FileExist(output) {
        success := 1
        break
    }
    Sleep, 250
}

CloseAndExit:
; Close viewer window
WinClose, ahk_pid %pid%
Process, WaitClose, %pid%, 5
ExitApp, success ? 0 : 1
