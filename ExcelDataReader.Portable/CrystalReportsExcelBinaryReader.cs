using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ExcelDataReader.Portable.Core;
using ExcelDataReader.Portable.Core.BinaryFormat;
using ExcelDataReader.Portable.Data;
using ExcelDataReader.Portable.Log;

namespace ExcelDataReader.Portable
{
    public class CrystalReportsExcelBinaryReader : ExcelBinaryReader
    {
        public CrystalReportsExcelBinaryReader(IDataHelper dataHelper) : base(dataHelper)
        {

        }

        Stream _file;
        XlsBiffStream _excelStream;
        bool _closed;
        string _errorMessage;
        ushort _version;
        bool IsV8 => _version >= 0x600;
        List<XlsWorksheet> _sheets;

        XlsHeader _header;
        XlsWorkbookGlobals _workbookGlobals;
        Encoding _encoding = Encoding.Unicode;

        #region public overriden methods from base class

        public override Encoding Encoding => _encoding;
        public override bool IsValid => string.IsNullOrWhiteSpace(_errorMessage);
        public override string ExceptionMessage => _errorMessage;
        public override string Name => _sheets.FirstOrDefault()?.Name;
        public override string VisibleState => _sheets.FirstOrDefault()?.VisibleState;
        public override int ResultsCount => _workbookGlobals?.Sheets.Count ?? 0;
        public override ReadOption ReadOption
        {
            get
            {
                return ReadOption.Loose;
            }
            set
            {
                if (value != ReadOption.Loose) throw new NotSupportedException("For Reporting Services only ReadOption.Loose is supported");
            }
        }

        public override async Task InitializeAsync(Stream fileStream)
        {
            _file = fileStream;
            _closed = false;

            await Task.Run(() => ReadWorkBookGlobals());
        }

        public override async Task LoadDataSetAsync(IDatasetHelper datasetHelper)
        {
            await LoadDataSetAsync(datasetHelper, false);
        }

        public override async Task LoadDataSetAsync(IDatasetHelper datasetHelper, bool convertOaDateTime)
        {
            datasetHelper.IsValid = IsValid;
            
            ConvertOaDate = convertOaDateTime;
            datasetHelper.CreateNew();

            await Task.Run(() => ReadAllSheets(datasetHelper));

            Close();
            datasetHelper.DatasetLoadComplete();
        }

        public override bool Read()
        {
            throw new NotSupportedException("Read is not supported, only LoadDataSet");
        }

        public override void Close()
        {
            _file.Dispose();
            _closed = true;
        }

        #endregion

        void ReadWorkBookGlobals()
        {
            try
            {
                _header = XlsHeader.ReadHeader(_file);
                var dir = new XlsRootDirectory(_header);
                var workbookEntry = dir.FindEntry(WORKBOOK) ?? dir.FindEntry(BOOK);

                if (workbookEntry == null)
                {
                    throw new Exception(Errors.ErrorStreamWorkbookNotFound);
                }

                if (workbookEntry.EntryType != STGTY.STGTY_STREAM)
                {
                    throw new Exception(Errors.ErrorWorkbookIsNotStream);
                }

                _excelStream = new XlsBiffStream(_header, workbookEntry.StreamFirstSector, workbookEntry.IsEntryMiniStream, dir, this);

                _workbookGlobals = new XlsWorkbookGlobals();

                _excelStream.Seek(0, SeekOrigin.Begin);

                var rec = _excelStream.Read();
                var bof = rec as XlsBiffBOF;

                if (bof == null || bof.Type != BIFFTYPE.WorkbookGlobals)
                {
                    throw new Exception(Errors.ErrorWorkbookGlobalsInvalidData);
                }

                var sst = false;

                _version = bof.Version;
                _sheets = new List<XlsWorksheet>();

                while (null != (rec = _excelStream.Read()))
                {
                    switch (rec.ID)
                    {
                        case BIFFRECORDTYPE.INTERFACEHDR:
                            _workbookGlobals.InterfaceHdr = (XlsBiffInterfaceHdr)rec;
                            break;
                        case BIFFRECORDTYPE.BOUNDSHEET:
                            var sheet = (XlsBiffBoundSheet)rec;

                            if (sheet.Type != XlsBiffBoundSheet.SheetType.Worksheet) break;

                            sheet.IsV8 = isV8();
                            //sheet.UseEncoding = Encoding;
                            this.Log().Debug("BOUNDSHEET IsV8={0}", sheet.IsV8);

                            _sheets.Add(new XlsWorksheet(_workbookGlobals.Sheets.Count, sheet));
                            _workbookGlobals.Sheets.Add(sheet);

                            break;
                        case BIFFRECORDTYPE.MMS:
                            _workbookGlobals.MMS = rec;
                            break;
                        case BIFFRECORDTYPE.COUNTRY:
                            _workbookGlobals.Country = rec;
                            break;
                        case BIFFRECORDTYPE.CODEPAGE:

                            var encoding = ((XlsBiffSimpleValueRecord)rec).Value;
                            //set encoding based on code page name
                            //PCL does not supported codepage numbers
                            _encoding = EncodingHelper.GetEncoding(encoding == 1200 ? (ushort)65001 : encoding);
                            break;
                        case BIFFRECORDTYPE.FONT:
                        case BIFFRECORDTYPE.FONT_V34:
                            _workbookGlobals.Fonts.Add(rec);
                            break;
                        case BIFFRECORDTYPE.FORMAT_V23:
                            {
                                var fmt = (XlsBiffFormatString)rec;
                                //fmt.UseEncoding = m_encoding;
                                _workbookGlobals.Formats.Add((ushort)_workbookGlobals.Formats.Count, fmt);
                            }
                            break;
                        case BIFFRECORDTYPE.FORMAT:
                            {
                                var fmt = (XlsBiffFormatString)rec;
                                _workbookGlobals.Formats.Add(fmt.Index, fmt);
                            }
                            break;
                        case BIFFRECORDTYPE.XF:
                        case BIFFRECORDTYPE.XF_V4:
                        case BIFFRECORDTYPE.XF_V3:
                        case BIFFRECORDTYPE.XF_V2:
                            _workbookGlobals.ExtendedFormats.Add(rec);
                            break;
                        case BIFFRECORDTYPE.SST:
                            _workbookGlobals.SST = (XlsBiffSST)rec;
                            sst = true;
                            break;
                        case BIFFRECORDTYPE.CONTINUE:
                            if (!sst) break;
                            var contSst = (XlsBiffContinue)rec;
                            _workbookGlobals.SST.Append(contSst);
                            break;
                        case BIFFRECORDTYPE.EXTSST:
                            _workbookGlobals.ExtSST = rec;
                            sst = false;
                            break;
                        case BIFFRECORDTYPE.PROTECT:
                        case BIFFRECORDTYPE.PASSWORD:
                        case BIFFRECORDTYPE.PROT4REVPASSWORD:
                            //IsProtected
                            break;
                        case BIFFRECORDTYPE.EOF:
                            _workbookGlobals.SST?.ReadStrings();
                            return;

                        default:
                            continue;
                    }
                }
            }
            catch (Exception ex)
            {
                SetError(ex.Message);
            }
        }

        void ReadAllSheets(IDatasetHelper datasetHelper)
        {
            if (_closed) return;
            foreach (var sheet in _sheets)
            {
                ReadSheet(sheet, datasetHelper);
            }
        }

        void ReadSheet(XlsWorksheet sheet, IDatasetHelper datasetHelper)
        {
            SheetGlobals header;
            try
            {
                header = ReadWorkSheetGlobals(sheet);
            }
            catch (Exception ex)
            {
                this.Log().Warn($"Failed to read globals for sheet {sheet.Name} ({sheet.Index}): {ex.Message}");
                datasetHelper.IsValid = false;
                return;
            }

            datasetHelper.CreateNewTable(sheet.Name);
            datasetHelper.AddExtendedPropertyToTable("visiblestate", sheet.VisibleState);

            var activeSheetCells = ReadWorkSheetData(header);
            datasetHelper.BeginLoadData();
            WriteColumns(datasetHelper, activeSheetCells, header);
            WriteDataToDataSet(activeSheetCells, datasetHelper);
            datasetHelper.EndLoadTable();
        }

        Dictionary<string, ExcelCell> ReadWorkSheetData(SheetGlobals header)
        {
            var sheetData = new Dictionary<string, ExcelCell>();

            foreach (var index in header.Index.DbCellAddresses)
            {
                var rowOffset = FindFirstDataCellOffset((int) index);
                if(rowOffset == -1) continue;

                ReadWorkSheetDataFromOffset(rowOffset, header, sheetData);
            }

            return sheetData;
        }

        void ReadWorkSheetDataFromOffset(int cellOffset, SheetGlobals sheetGlobals, Dictionary<string, ExcelCell> sheetData)
        {
            while (cellOffset < _excelStream.Size)
            {
                var rec = _excelStream.ReadAt(cellOffset);
                cellOffset += rec.Size;

                if ((rec is XlsBiffDbCell) || (rec is XlsBiffMSODrawing)) { continue; }
                if (rec is XlsBiffEOF) { return; }

                var cell = rec as XlsBiffBlankCell;
                if ((null == cell) || (cell.ColumnIndex >= sheetGlobals.Columns)) continue;

                AddCell(sheetData, cell, sheetGlobals);
            }
        }

        void WriteDataToDataSet(Dictionary<string, ExcelCell> cells, IDatasetHelper datasetHelper)
        {
            var sheetCells = cells.Values.GroupBy(c => c.Row).OrderBy(c => c.Key);
            foreach (var row in sheetCells)
            {
                var columns = row.Max(c => c.Column);
                var data = new object[columns + 1];
                for (var col = 0; col <= columns; col++)
                {
                    data[col] = row.FirstOrDefault(c => c.Column == col)?.Value;
                }
                datasetHelper.AddRow(data);
            }
        }

        void WriteColumns(IDatasetHelper datasetHelper, Dictionary<string, ExcelCell> sheetData, SheetGlobals header)
        {
            var firstRow = sheetData.Where(c => c.Value.Row == 0).ToList();
            for (var index = 0; index < header.Columns; index++)
            {
                string name = null; //by default use no column names
                if (IsFirstRowAsColumnNames)
                {
                    name = firstRow.FirstOrDefault(c => c.Value.Column == index) //Search for matching excel cell
                        .Value? //If it is found and not null
                        .Value? //Read the value from the cell
                        .ToString(); //And convert it into a string
                    if (string.IsNullOrWhiteSpace(name)) //If this name is null
                    {
                        name = string.Concat(COLUMN, index); //then use default names like: Column1, Column2     
                    }
                }
                datasetHelper.AddColumn(name);
            }
            if (IsFirstRowAsColumnNames)
            {
                //Clean up the cells from the first row, because we used them as columns
                foreach (var col in firstRow)
                {
                    sheetData.Remove(col.Key);
                }
            }
        }

        int FindFirstDataCellOffset(int startOffset)
        {
            //seek to the first dbcell record
            var record = _excelStream.ReadAt(startOffset);
            while (!(record is XlsBiffDbCell))
            {
                if (_excelStream.Position >= _excelStream.Size) return -1;
                if (record is XlsBiffEOF) return -1;

                record = _excelStream.Read();
            }

            var startCell = (XlsBiffDbCell)record;
            var offs = startCell.RowAddress;

            do
            {
                var row = _excelStream.ReadAt(offs) as XlsBiffRow;
                if (row == null) break;
                offs += row.Size;
            } while (true);

            return offs;
        }

        void AddCell(IDictionary<string, ExcelCell> values, XlsBiffBlankCell cell, SheetGlobals sheetGlobals)
        {
            if (cell.ID == BIFFRECORDTYPE.MULRK)
            {
                var multiRecordsCell = (XlsBiffMulRKCell) cell;
                for (var col = cell.ColumnIndex; col <= multiRecordsCell.LastColumnIndex; col++)
                {
                    var newValue = multiRecordsCell.GetValue(col);
                    var mergedCellValue = ConvertOaDateTime(newValue, multiRecordsCell.GetXF(col));
                    AddCell(values, mergedCellValue, col, cell.RowIndex, sheetGlobals);
                }
            }
            else
            {
                var value = ReadCellValue(cell);
                AddCell(values, value, cell.ColumnIndex, cell.RowIndex, sheetGlobals);
            }
        }

        object ReadCellValue(XlsBiffBlankCell cell)
        {
            double dValue;
            switch (cell.ID)
            {
                case BIFFRECORDTYPE.BOOLERR:
                    if (cell.ReadByte(7) == 0)
                        return cell.ReadByte(6) != 0;
                    break;
                case BIFFRECORDTYPE.BOOLERR_OLD:
                    if (cell.ReadByte(8) == 0)
                        return cell.ReadByte(7) != 0;
                    break;
                case BIFFRECORDTYPE.INTEGER:
                case BIFFRECORDTYPE.INTEGER_OLD:
                    return ((XlsBiffIntegerCell)cell).Value;
                case BIFFRECORDTYPE.NUMBER:
                case BIFFRECORDTYPE.NUMBER_OLD:
                    dValue = ((XlsBiffNumberCell)cell).Value;
                    return ConvertOaDateTime(dValue, cell.XFormat);
                case BIFFRECORDTYPE.LABEL:
                case BIFFRECORDTYPE.LABEL_OLD:
                case BIFFRECORDTYPE.RSTRING:
                    return ((XlsBiffLabelCell)cell).Value;
                case BIFFRECORDTYPE.LABELSST:
                    return _workbookGlobals.SST.GetString(((XlsBiffLabelSSTCell)cell).SSTIndex);
                case BIFFRECORDTYPE.RK:
                    dValue = ((XlsBiffRKCell)cell).Value;
                    return ConvertOaDateTime(dValue, cell.XFormat);
                case BIFFRECORDTYPE.MULRK:
                    
                case BIFFRECORDTYPE.BLANK:
                case BIFFRECORDTYPE.BLANK_OLD:
                case BIFFRECORDTYPE.MULBLANK:
                    // Skip blank cells
                    break;
                case BIFFRECORDTYPE.FORMULA:
                case BIFFRECORDTYPE.FORMULA_OLD:
                    var oValue = ((XlsBiffFormulaCell)cell).Value;
                    if (oValue is FORMULAERROR)
                    {
                        oValue = null;
                    }
                    else
                    {
                        oValue = ConvertOaDateTime(oValue, (cell.XFormat));//date time offset
                    }
                    return oValue;
            }

            return null;
        }

        object ConvertOaDateTime(double value, ushort xFormat)
        {
            return ConvertOaDate ? 
                TryConvertOaDateTime(value, xFormat) : 
                value;
        }

        object ConvertOaDateTime(object value, ushort xFormat)
        {
            if (!ConvertOaDate) return value;
            if (value == null) return null;

            double dValue;
            return double.TryParse(value.ToString(), out dValue) ? 
                TryConvertOaDateTime(dValue, xFormat) : 
                value;
        }

        object TryConvertOaDateTime(double value, ushort xFormat)
        {
            ushort format;
            if (xFormat < _workbookGlobals.ExtendedFormats.Count)
            {
                var rec = _workbookGlobals.ExtendedFormats[xFormat];
                switch (rec.ID)
                {
                    case BIFFRECORDTYPE.XF_V2:
                        format = (ushort)(rec.ReadByte(2) & 0x3F);
                        break;
                    case BIFFRECORDTYPE.XF_V3:
                        if ((rec.ReadByte(3) & 4) == 0)
                            return value;
                        format = rec.ReadByte(1);
                        break;
                    case BIFFRECORDTYPE.XF_V4:
                        if ((rec.ReadByte(5) & 4) == 0)
                            return value;
                        format = rec.ReadByte(1);
                        break;
                    default:
                        if ((rec.ReadByte(_workbookGlobals.Sheets[_workbookGlobals.Sheets.Count - 1].IsV8 ? 9 : 7) & 4) == 0)
                            return value;
                        format = rec.ReadUInt16(2);
                        break;
                }
            }
            else
            {
                format = xFormat;
            }

            switch (format)
            {
                // numeric built in formats
                case 0: //"General";
                case 1: //"0";
                case 2: //"0.00";
                case 3: //"#,##0";
                case 4: //"#,##0.00";
                case 5: //"\"$\"#,##0_);(\"$\"#,##0)";
                case 6: //"\"$\"#,##0_);[Red](\"$\"#,##0)";
                case 7: //"\"$\"#,##0.00_);(\"$\"#,##0.00)";
                case 8: //"\"$\"#,##0.00_);[Red](\"$\"#,##0.00)";
                case 9: //"0%";
                case 10: //"0.00%";
                case 11: //"0.00E+00";
                case 12: //"# ?/?";
                case 13: //"# ??/??";
                case 0x30:// "##0.0E+0";

                case 0x25:// "_(#,##0_);(#,##0)";
                case 0x26:// "_(#,##0_);[Red](#,##0)";
                case 0x27:// "_(#,##0.00_);(#,##0.00)";
                case 40:// "_(#,##0.00_);[Red](#,##0.00)";
                case 0x29:// "_(\"$\"* #,##0_);_(\"$\"* (#,##0);_(\"$\"* \"-\"_);_(@_)";
                case 0x2a:// "_(\"$\"* #,##0_);_(\"$\"* (#,##0);_(\"$\"* \"-\"_);_(@_)";
                case 0x2b:// "_(\"$\"* #,##0.00_);_(\"$\"* (#,##0.00);_(\"$\"* \"-\"??_);_(@_)";
                case 0x2c:// "_(* #,##0.00_);_(* (#,##0.00);_(* \"-\"??_);_(@_)";
                    return value;

                // date formats
                case 14: //this.GetDefaultDateFormat();
                case 15: //"D-MM-YY";
                case 0x10: // "D-MMM";
                case 0x11: // "MMM-YY";
                case 0x12: // "h:mm AM/PM";
                case 0x13: // "h:mm:ss AM/PM";
                case 20: // "h:mm";
                case 0x15: // "h:mm:ss";
                case 0x16: // string.Format("{0} {1}", this.GetDefaultDateFormat(), this.GetDefaultTimeFormat());

                case 0x2d: // "mm:ss";
                case 0x2e: // "[h]:mm:ss";
                case 0x2f: // "mm:ss.0";
                    return Helpers.ConvertFromOATime(value);
                case 0x31:// "@";
                    return value.ToString();

                default:
                    XlsBiffFormatString fmtString;
                    if (_workbookGlobals.Formats.TryGetValue(format, out fmtString))
                    {
                        var fmt = fmtString.Value;
                        var formatReader = new FormatReader() { FormatString = fmt };
                        if (formatReader.IsDateFormatString())
                            return Helpers.ConvertFromOATime(value);
                    }
                    return value;
            }
        }

        void AddCell(IDictionary<string, ExcelCell> cells, object value, ushort columnIndex, ushort rowIndex, SheetGlobals sheetGlobals)
        {
            if (rowIndex >= sheetGlobals.Rows || columnIndex >= sheetGlobals.Columns)
            {
                LogManager.Log(this).Warn("Cannot write value in cell({0},{1}): {2}", rowIndex, columnIndex, value);
                return;
            }
            
            var key = $"{rowIndex}:{columnIndex}";
            if (!cells.ContainsKey(key))
            {
                cells.Add(key, new ExcelCell(value, columnIndex, rowIndex));
                return;
            }
            var existingCell = cells[key];
            var existingValue = existingCell.Value?.ToString();
            var newValue = value?.ToString();
            if (existingValue == newValue) return;
            this.Log().Info($"Overwrite value '{existingValue}' with '{newValue}' on {rowIndex}:{columnIndex}");
            existingCell.Value = value;
        }

        SheetGlobals ReadWorkSheetGlobals(XlsWorksheet sheet)
        {
            var data = new SheetGlobals();

            _excelStream.Seek((int)sheet.DataOffset, SeekOrigin.Begin);

            var bof = _excelStream.Read() as XlsBiffBOF;
            if (bof == null || bof.Type != BIFFTYPE.Worksheet) throw new Exception("Failed to read XLS BOF");

            var rec = _excelStream.Read();
            if (rec == null) throw new Exception("Failed to read first record");
            if (rec is XlsBiffIndex)
            {
                data.Index = rec as XlsBiffIndex;
            }
            else if (rec is XlsBiffUncalced)
            {
                // Sometimes this come before the index...
                data.Index = _excelStream.Read() as XlsBiffIndex;
            }


            if (data.Index == null)
            {
                throw new Exception("Failed to read the Index record, which is required");
            }

            data.Index.IsV8 = IsV8;
            this.Log().Debug("INDEX IsV8={0}", data.Index.IsV8);

            XlsBiffRecord trec;
            XlsBiffDimensions dims = null;

            do
            {
                trec = _excelStream.Read();
                if (trec.ID != BIFFRECORDTYPE.DIMENSIONS) continue;

                dims = (XlsBiffDimensions)trec;
                break;
            } while (trec.ID != BIFFRECORDTYPE.ROW);

            XlsBiffRow rowRecord = null;
            while (rowRecord == null)
            {
                if (_excelStream.Position >= _excelStream.Size)
                    break;
                var thisRec = _excelStream.Read();

                this.Log().Debug("finding rowRecord offset {0}, rec: {1}", thisRec.Offset, thisRec.ID);
                if (thisRec is XlsBiffEOF)
                    break;
                rowRecord = thisRec as XlsBiffRow;
            }

            if (rowRecord != null) this.Log().Debug("Got row {0}, rec: id={1},rowindex={2}, rowColumnStart={3}, rowColumnEnd={4}", rowRecord.Offset, rowRecord.ID, rowRecord.RowIndex, rowRecord.FirstDefinedColumn, rowRecord.LastDefinedColumn);

            data.Row = rowRecord;

            if (dims != null)
            {
                dims.IsV8 = isV8();
                this.Log().Debug("dims IsV8={0}", dims.IsV8);
                data.Columns = dims.LastColumn - 1;

                //handle case where sheet reports last column is 1 but there are actually more
                if (data.Columns <= 0 && rowRecord != null)
                {
                    data.Columns = rowRecord.LastDefinedColumn;
                }

                data.Rows = (int)dims.LastRow;
                sheet.Dimensions = dims;
            }
            else
            {
                data.Columns = 256;
                data.Rows = (int)data.Index.LastExistingRow;
            }

            if (data.Index.LastExistingRow <= data.Index.FirstExistingRow)
            {
                throw new Exception($"Last row ({data.Index.LastExistingRow}) <= first row ({data.Index.FirstExistingRow})");
            }

            if (data.Row == null)
            {
                throw new Exception("Failed to read the first data record");
            }

            return data;
        }

        void SetError(string message)
        {
            _errorMessage = message;
            Close();
        }
    }

    class SheetGlobals
    {
        public XlsBiffIndex Index { get; set; }
        public XlsBiffRow Row { get; set; }
        public int Columns { get; set; }
        public int Rows { get; set; }
    }

    class ExcelCell
    {
        public int Column { get; private set; }
        public int Row { get; private set; }
        public object Value { get; set; }

        public ExcelCell(object value, int column, int row)
        {
            Value = value;
            Column = column;
            Row = row;
        }
    }
}