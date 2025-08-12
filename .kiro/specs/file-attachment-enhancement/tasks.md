# Implementation Plan

- [ ] 1. Create backend DuckDB processing utility





  - Create `/api/utils/process_duckdb.py` file with `get_cols()` function
  - Implement DuckDB file validation and column extraction logic
  - Add error handling for invalid files and processing failures
  - Include file cleanup functionality for temporary files
  - _Requirements: 3.1, 3.2, 3.3, 5.2_

- [x] 2. Add backend endpoint for DuckDB file uploads





  - Create new `POST /api/upload-duckdb` endpoint in `api/index.py`
  - Implement multipart file upload handling
  - Add file size validation and security checks
  - Route DuckDB files to the processing utility
  - Return column information in structured JSON response
  - _Requirements: 3.1, 3.4, 5.1_

- [x] 3. Create reusable attachment button component






  - Create `components/ui/attachment-button.tsx` component
  - Implement props interface for file type, accept filters, and callbacks
  - Add visual styling and hover states for different file types
  - Include tooltip functionality for file type indication
  - _Requirements: 4.1, 4.2, 4.3_

- [x] 4. Modify multimodal input component for dual file handling






  - Update `components/multimodal-input.tsx` to replace single button with two dedicated buttons
  - Add separate state management for CSV and DuckDB files
  - Implement file type validation for each button (CSV accepts only .csv, DuckDB accepts only .duckdb/.db)
  - Add error handling and user feedback for invalid file types
  - _Requirements: 1.1, 1.4, 2.1, 2.4, 4.4_

- [x] 5. Implement DuckDB file upload workflow in frontend






  - Add DuckDB file upload logic to multimodal input component
  - Implement direct file upload to backend without client-side processing
  - Add loading states and progress indication for DuckDB uploads
  - Handle server responses and display column information
  - _Requirements: 2.2, 2.3, 4.3_

- [ ] 6. Update CSV file handling to use dedicated button
  - Modify existing CSV processing logic to work with dedicated CSV button
  - Ensure CSV files continue to use existing content extraction and JSON schema conversion
  - Maintain backward compatibility with current CSV processing pipeline
  - Add file type validation specific to CSV button
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 7. Enhance preview attachment component for dual file types
  - Update `components/preview-attachment.tsx` to handle both CSV and DuckDB files
  - Display file type-specific information (JSON schema for CSV, column names for DuckDB)
  - Add loading states for DuckDB processing
  - Maintain existing preview functionality for CSV files
  - _Requirements: 4.3, 4.4_

- [ ] 8. Add comprehensive error handling across components
  - Implement error handling for DuckDB upload failures in frontend
  - Add user-friendly error messages for file validation failures
  - Ensure CSV error handling maintains existing behavior
  - Add network error handling with retry options for DuckDB uploads
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ] 9. Update form submission logic for dual file types
  - Modify submit form logic in multimodal input to handle both CSV and DuckDB attachments
  - Ensure CSV files continue to send JSON schema data with user prompt
  - Implement DuckDB file handling in message submission
  - Add validation to prevent submission with invalid attachments
  - _Requirements: 1.3, 2.2, 2.3_

- [ ] 10. Add unit tests for backend DuckDB processing
  - Write tests for `get_cols()` function with valid DuckDB files
  - Test error handling for invalid and corrupted DuckDB files
  - Test file cleanup functionality
  - Test column extraction accuracy with sample databases
  - _Requirements: 3.2, 3.3_

- [ ] 11. Add frontend component tests for dual file handling
  - Test file type validation for both CSV and DuckDB buttons
  - Test error message display for invalid file types
  - Test upload progress indication for DuckDB files
  - Test state management for dual file attachments
  - _Requirements: 1.4, 2.4, 4.1, 4.2_

- [ ] 12. Integration testing for end-to-end file workflows
  - Test complete CSV file processing workflow (existing functionality)
  - Test complete DuckDB file upload and column extraction workflow
  - Test error scenarios and user feedback
  - Test concurrent file handling and state management
  - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3_