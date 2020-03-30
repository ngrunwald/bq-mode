;;; bq-mode.el --- Major mode for Google Big Query

;; Copyright (C) 2020 Nils Grunwald

;; Author: GRUNWALD Nils
;; URL: http://github.com/ngrunwald/ob-bigquery
;; Version: 0.1.0
;; Package-Requires: ((spinner "1.7.3") (aio "1.0") (om "1.2.0")
;;                    (ht "2.3") (s "1.12.0") (transient "0.2.0")
;;                    (csv-mode "1.12"))
;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:
;;
;; Major mode for Google Big Query
;;

;;; Code:
(require 'json)
(require 'aio)
(require 'om)
(require 'spinner)
(require 'ht)
(require 's)
(require 'transient)
(require 'csv-mode)

(setq lexical-binding t)

(aio-defun bq-get-datasets-list ()
  "Get the list of datasets in current project."
  (-let* ((p-result (bq-async-json-command "ls --max_results=10000")))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(aio-defun bq-get-tables-list (dataset)
  "Get the list of tables in given dataset."
  (-let* ((p-result (bq-async-json-command (format "ls --max_results=10000 %s" dataset))))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(aio-defun bq-get-entity-info (id)
  (-let* ((p-result (bq-async-json-command (format "show %s" id))))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(defun bq--set (hmap key val)
  (ht-set! hmap key val)
  hmap)

(defun bq--analyze-field (f-spec &optional path)
  (-let* ((nam (ht-get f-spec "name"))
          (current (bq--set f-spec "path" (append path (list nam)))))
    (-if-let (fields (ht-get f-spec "fields"))
        (append (list current) (-mapcat (lambda (f) (bq--analyze-field f (append path (list nam)))) fields))
      (list current))))

(defun bq--analyze-schema (s)
  (-mapcat 'bq--analyze-field (ht-get s "fields")))

(defun bq--to-human-size (x)
  (-> x
      (string-to-number)
      (file-size-human-readable)))

(defun bq--format-date (d)
  (-> d
       (string-to-number)
       (/ 1000)
       ((lambda (x) (format-time-string "%F %T" x)))))

(defun bq--add-number-grouping (num &optional separator)
  "Add commas to NUMBER and return it as a string.
   Optional SEPARATOR is the string to use to separate groups.
   It defaults to a comma."
  (let ((op (or separator ",")))
    (while (string-match "\\(.*[0-9]\\)\\([0-9][0-9][0-9].*\\)" num)
      (setq num (concat
                 (match-string 1 num) op
                 (match-string 2 num))))
    num))

(defun bq--json-bool-to-string (x)
  (if (eq :json-false x)
      "false"
    "true"))

(defconst bq-info-keys
  (list
   `(("id") . ("Id" . identity))
   `(("type") . ("Type" . identity))
   `(("timePartitioning" "type") . ("Time Partitioning Type" . identity))
   `(("view" "useLegacySql") . ("Use Legacy SQL" . bq--json-bool-to-string))
   `(("numRows") . ("Rows" . bq--add-number-grouping))
   `(("numBytes") . ("Size" . bq--to-human-size))
   `(("numLongTermBytes") . ("Long Term Size" . bq--to-human-size))
   `(("creationTime") . ("Creation Time" . bq--format-date))
   `(("lastModifiedTime") . ("Last Modified Time" . bq--format-date))
   `(("location") . ("Location" . identity))))

(aio-defun bq-entity-info (id)
  (interactive)
  (-let* ((buffer "*BigQuery-Info*")
          (data-p (bq-get-entity-info id)))
    (switch-to-buffer buffer)
    (read-only-mode -1)
    (erase-buffer)
    (orgtbl-mode)
    (spinner-start 'progress-bar)
    (aio-await data-p)
    (condition-case err
        (-let* ((data (funcall (aio-result data-p)))
                (info-rows (cl-loop for elt in bq-info-keys
                                    when (condition-case _
                                             (apply 'ht-get* data (car elt))
                                           (error nil))
                                    collect
                                    (-let [(ks . (label . f)) elt]
                                      (list label (funcall f (apply 'ht-get* data ks)))))))

          (save-excursion
            (switch-to-buffer buffer)
            (-when-let (desc (ht-get data "description"))
              (insert (propertize "Description" 'face 'bold-italic))
              (insert "\n\n")
              (insert desc)
              (insert "\n\n"))

            (-when-let (query (condition-case _
                                  (ht-get* data "view" "query")
                                (error nil)))
              (insert (propertize "View Query" 'face 'bold-italic))
              (insert "\n\n")
              (insert query)
              (insert "\n\n"))

            (insert (propertize "Information" 'face 'bold-italic))
            (insert "\n\n")
            (->> (apply 'om-build-table! info-rows)
                 (om-to-trimmed-string)
                 (insert))
            (insert "\n\n")

            (-when-let (schema (ht-get data "schema"))
              (-let* ((fields (bq--analyze-schema schema))
                      (schema-rows (cl-loop for elt in fields
                                            collect
                                            (-> (list (s-join "." (ht-get elt "path"))
                                                      (ht-get elt "type")
                                                      (ht-get elt "mode"))
                                                (append (-when-let (desc (ht-get elt "description"))
                                                          (list desc)))))))
                (insert (propertize "Schema" 'face 'bold-italic))
                (insert "\n\n")
                (->> (apply 'om-build-table! schema-rows)
                     (om-to-trimmed-string)
                     (insert))
                (insert "\n")))
            (read-only-mode)
            (spinner-stop)))
      (error
       (spinner-stop)
       (message err)))))

(define-derived-mode bq-datasets-mode tablist-mode "BigQuery Datasets"
  "Mode for exploring the datasets in a project"
  (-let ((columns [("Dataset" 50 t) ("Location" 20 t)])
         (rows (-map (lambda (it)
                       `(,(ht-get it "id") [,(ht-get* it "datasetReference" "datasetId")
                                            ,(ht-get it "location")]))
                     bq-current-datasets-list)))
    (setq tabulated-list-format columns)
    (setq tabulated-list-entries rows)
    (tabulated-list-init-header)
    (tabulated-list-print)))

(define-key bq-datasets-mode-map (kbd "<return>") 'bq-tables-from-list)
(define-key bq-datasets-mode-map (kbd "i") 'bq-entity-info-from-list)

(define-derived-mode bq-tables-mode tablist-mode "BigQuery Tables"
  "Mode for exploring the tables in a dataset"
  (-let ((columns [("Table" 50 t) ("Type" 30 t) ("Creation Date" 30 t)])
         (rows (-map (lambda (it)
                       `(,(ht-get it "id") [,(ht-get* it "tableReference" "tableId")
                                            ,(ht-get it "type")
                                            ,(-> it (ht-get "creationTime") (bq--format-date))]))
                     bq-current-tables-list)))
    (setq tabulated-list-format columns)
    (setq tabulated-list-entries rows)
    (tabulated-list-init-header)
    (tabulated-list-print)))

(define-key bq-tables-mode-map (kbd "<return>") 'bq-entity-info-from-list)

(defun bq-async-json-command (cmd &optional raw-format)
  "Get entity info as pretty text."
  (condition-case err
      (-let* ((temp-buffer "*BigQuery-Output*")
              (proc-name "*BigQuery-Output-Proc*")
              (promise (aio-promise))
              (prefix (concat "bq --quiet --format=" (if raw-format raw-format "json") " "))
              (command (concat prefix cmd))
              (proc (start-process-shell-command proc-name temp-buffer (concat prefix cmd))))
        (set-process-sentinel proc
                              (lambda (process event)
                                (save-excursion
                                  (switch-to-buffer temp-buffer)
                                  (-let* ((result-str (buffer-string))
                                          (json-object-type 'hash-table)
                                          (result (if raw-format result-str (json-read-from-string result-str))))
                                    (aio-resolve promise (lambda () result))
                                    (kill-buffer)))))
        promise)
    (error (message "ERR => %s"err))))

(aio-defun bq-datasets ()
  (interactive)
  (-let ((buffer "*BigQuery-Datasets*"))
    (switch-to-buffer buffer)
    (setq bq-current-datasets-list '())
    (bq-datasets-mode)
    (spinner-start 'progress-bar)
    (-let ((p-datasets (bq-get-datasets-list)))
      (aio-await p-datasets)
      (save-excursion
        (switch-to-buffer buffer)
        (setq bq-current-datasets-list (funcall (aio-result p-datasets)))
        (spinner-stop)
        (bq-datasets-mode)))))

(aio-defun bq-tables-from-list ()
  (interactive)
  (-let ((id (tabulated-list-get-id))
         (buffer "*BigQuery-Tables*"))
    (switch-to-buffer buffer)
    (setq bq-current-tables-list '())
    (bq-tables-mode)
    (spinner-start 'progress-bar)
    (-let ((p-tables (bq-get-tables-list id)))
      (aio-await p-tables)
      (save-excursion
        (switch-to-buffer buffer)
        (setq bq-current-tables-list (funcall (aio-result p-tables)))
        (spinner-stop)
        (bq-tables-mode)))))

(defun bq-entity-info-from-list ()
  ""
  (interactive)
  (-let ((id (tabulated-list-get-id)))
    (bq-entity-info id)))

(defconst bq-query-processors
  (ht ("--output=json" (ht (:format "prettyjson")
                           (:renderer 'insert)
                           (:mode 'json-mode)))
      ("--output=table" (ht (:format "pretty")
                            (:renderer 'insert)
                            (:mode 'orgtbl-mode)))
      ("--output=csv" (ht (:format "csv")
                          (:renderer 'insert)
                          (:mode 'csv-mode)))))

(define-transient-command bq-query-transient ()
  "Sends an SQL query to Big Query."
  ["Arguments"
   ("-l" "Do not use Legacy SQL" "--nouse_legacy_sql")
   ("-L" "Allow Large Results" "--allow_large_results")
   (bq-query:--output)
   ("-F" "Do not Flatten Results" "--noflatten_results")
   ("-b" "Batch Mode" "--batch")
   ("-n" "Max Number of Rows" "--max_rows=")
   ("-s" "Starting Row" "--start_row=")]
  ["Actions"
   ("r" "Query" bq-send-paragraph)])

(define-infix-argument bq-query:--output ()
  :description "Output Format"
  :class 'transient-option
  :key "-o"
  :argument "--output="
  :choices '("json" "table" "csv"))

(aio-defun bq-send-region (start end args)
  (condition-case err
      (-let* ((query (buffer-substring-no-properties start end))
              (pass-options (-remove (lambda (x) (s-matches? "--output=" x)) args))
              (output (or (car (-filter (lambda (x) (s-matches? "--output=" x)) args))
                          (if (member "--nouse_legacy_sql" args)
                              "--output=json"
                            "--output=table")))
              (p-result (bq-async-json-command (format "query %s '%s'" (s-join " " pass-options) query)
                                               (ht-get* bq-query-processors output :format))))
        (switch-to-buffer "*BigQuery-Results*")
        (read-only-mode -1)
        (erase-buffer)
        (funcall (ht-get* bq-query-processors output :mode))
        (spinner-start 'progress-bar)
        (aio-await p-result)
        (-let ((result (funcall (aio-result p-result))))
          (save-excursion
            (switch-to-buffer "*BigQuery-Results*")
            (funcall (ht-get* bq-query-processors output :renderer) result)
            (spinner-stop)
            (read-only-mode))))
    (error (message "ERR => %s" err))))

(defun bq-send-paragraph (&optional args)
  "Send the current paragraph to Big Query, getting ARGS from transient."
  (interactive
   (list (transient-args 'bq-query-transient)))
  (let ((start (save-excursion
                 (backward-paragraph)
                 (point)))
        (end (save-excursion
               (forward-paragraph)
               (point))))
    (bq-send-region start end args)))

(provide 'bq-mode)
;;; bq-mode ends here
