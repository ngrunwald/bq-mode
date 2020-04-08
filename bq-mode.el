;;; bq-mode.el --- Major mode for Google Big Query -*- lexical-binding: t -*-

;; Copyright (C) 2020 Nils Grunwald

;; Author: GRUNWALD Nils
;; URL: http://github.com/ngrunwald/ob-bigquery
;; Version: 0.1.0
;; Package-Requires: ((spinner "1.7.3") (aio "1.0") (om "1.2.0")
;;                    (ht "2.3") (s "1.12.0") (transient "0.2.0")
;;                    (csv-mode "1.12") (company "0.9.12") (pcache "0.4.2"))
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
(require 'company)
(require 'pcache)
(require 'seq)
(require 'cl-lib)

(defun bq-get-current-project ()
  (-let ((project (car (cdr (s-split "\n" (shell-command-to-string "gcloud config get-value project"))))))
    (setq bq-current-project project)
    project))

(defun bq-project-cache-repo (project)
  (pcache-repository (concat "bq-" project)))

(defvar bq-current-project nil)

(defun bq-clear-cache (&optional project)
  (interactive)
  (pcache-destroy-repository (concat "bq-" (or project (bq-get-current-project)))))

(defconst list-processors
  (ht (:datasets (ht (:format-fn (lambda (&optional project &rest _)
                                   (format "ls --project_id=%s --max_results=10000"
                                           (or project (bq-get-current-project)))))
                     (:cache-key-fn (lambda (&rest _) (intern "--bq-datasets")))))
      (:tables (ht (:format-fn (lambda (project dataset &rest _)
                                 (format "ls --project_id=%s --max_results=10000 %s"
                                         (or project (bq-get-current-project))
                                         dataset)))
                   (:cache-key-fn (lambda (dataset &rest _)
                                    (intern (format "--bq-dataset--%s"
                                                    (bq-remove-project dataset)))))))
      (:projects (ht (:format-fn (lambda (&rest _)
                                   "ls -p --max_results=10000"))
                     (:cache-key-fn (lambda (&rest _) (intern "--bq-projects")))))))

(defun bq-remove-project (id)
  (s-replace-regexp "^[^:]+:" "" id))

(defun bq-extract-project (id)
  (elt (s-match "^\\([^:]+\\):" id) 1))

(defun bq-remove-dataset (id)
  (s-replace-regexp "^[^\.]+\." "" id))

(defun bq-extract-dataset (id)
  (elt (s-match "^\\([^\.]+\\)\." id) 1))

(aio-defun bq-get-entities-list (req-type &optional project dataset refresh)
  "Get the list of entities in current project."
  (-let* ((procs (ht-get list-processors req-type))
          (current-project (or project
                               (if refresh
                                   (bq-get-current-project)
                                 (or bq-current-project (bq-get-current-project)))))
          (cache-key (funcall (ht-get procs :cache-key-fn) dataset))
          (from-cache (pcache-get (bq-project-cache-repo current-project) cache-key)))
    (if (and from-cache (not refresh))
        from-cache
      (-let* ((p-result (bq-async-json-command (funcall (ht-get procs :format-fn) current-project dataset)))
              (_ (aio-await p-result))
              (data (funcall (aio-result p-result))))
        (pcache-put (bq-project-cache-repo current-project) cache-key data)
        data))))

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
        (append (list current)
                (-mapcat
                 (lambda (f)
                   (bq--analyze-field f (append path (list nam))))
                 fields))
      (list current))))

(defun bq--analyze-schema (dataset table s)
  (->> (ht-get s "fields")
       (-mapcat 'bq--analyze-field)
       (--map (bq--set it "dataset" dataset))
       (--map (bq--set it "table" table))))

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
          (data-p (bq-get-entity-info id))
          (project (or (bq-extract-project id)
                       bq-current-project
                       (bq-get-current-project))))
    (switch-to-buffer buffer)
    (read-only-mode -1)
    (erase-buffer)
    (orgtbl-mode)
    (spinner-start 'progress-bar)
    (aio-await data-p)
    (condition-case err
        (-let* ((data (funcall (aio-result data-p)))
                (table-id (ht-get* data "tableReference" "tableId"))
                (dataset-id (ht-get* data "tableReference" "datasetId"))
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
              (-let* ((fields (bq--analyze-schema dataset-id table-id schema))
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
                (insert "\n")
                (pcache-put (bq-project-cache-repo project)
                            (intern (format "--bq-table--%s" (bq-remove-project id)))
                            fields)))
            (read-only-mode)
            (spinner-stop)))
      (error
       (spinner-stop)
       (message err)))))

;; (bq-entity-info "daily_addiary.addiary_pt")

(define-derived-mode bq-datasets-mode tablist-mode "BigQuery Datasets"
  "Mode for exploring the datasets in a project"
  :group 'bq-mode
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
  :group 'bq-mode
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

(define-derived-mode bq-query-mode sql-mode "BigQuery Query"
  "Mode for writing and executing Big Query queries."
  :group 'bq-mode
  (setq comment-start "--")
  (setq comment-end "")
  (modify-syntax-entry ?. "_")
  (modify-syntax-entry ?: "_"))

(define-key bq-tables-mode-map (kbd "<tab>") 'company-complete)

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
    (-let ((p-datasets (bq-get-entities-list :datasets)))
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
    (-let ((p-tables (bq-get-entities-list :tables nil id)))
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

(defun bq--table-context-p ()
  (interactive)
  (save-excursion
    (while (not (or (char-equal (char-before) ?\n)
                    (char-equal (char-before) ?\s)))
        (backward-char))
    (-let ((case-fold-search t))
      (or (looking-back "\\bfrom[\s\n]+" 10 t)
          (looking-back "\\bjoin[\s\n]+" 10 t)
          (looking-back "\\bunnest([\s\n]+" 10 t)))))

(defun bq-query-completions-fields-candidates (tables)
  (seq-mapcat
   (lambda (table)
     (-let ((data (pcache-get (bq-project-cache-repo (or bq-current-project
                                                            (bq-get-current-project)))
                                 (intern (format "--bq-table--%s" table)))))
       (when data
           (--mapcat (-let* ((name (s-join "." (ht-get it "path")))
                             (dataset (ht-get it "dataset"))
                             (description (ht-get it "description"))
                             (mode (ht-get it "mode"))
                             (type (if (string= mode "REPEATED")
                                       (concat mode " " (ht-get it "type"))
                                     (ht-get it "type"))))
                       (when (not (string= "RECORD" (ht-get it "type")))
                         (list (propertize name
                                           'annotation (format " [%s] <= %s" type table)
                                           'meta description)
                               (propertize (concat (bq-remove-dataset table) "." name)
                                           'annotation (format " [%s] <= %s" type dataset)
                                           'meta description))))
                     data))))
   tables))


;; (pcache-get (bq-project-cache-repo (or bq-current-project
;;                                        (bq-get-current-project)))
;;             (intern (format "--bq-table--%s" "catalolo.product")))

;; (bq-query-completions-fields-candidates (list "curiosity.availabilities"))

(defun bq-query-completions-datasets-candidates (&optional project)
  (-let ((data (pcache-get (bq-project-cache-repo (or project
                                                      bq-current-project
                                                      (bq-get-current-project)))
                           (intern "--bq-datasets"))))
    (--map (-> it
               (ht-get* "datasetReference" "datasetId")
               (concat ".")
               (propertize 'annotation " [DATASET]"))
           data)))

(defun bq-query-completions-tables-candidates (dataset)
  (-let ((data (pcache-get (bq-project-cache-repo (or bq-current-project
                                                      (bq-get-current-project)))
                           (intern (format "--bq-dataset--%s" (bq-remove-project dataset))))))
    (--map (-> (format "%s.%s"
                       (ht-get* it "tableReference" "datasetId")
                       (ht-get* it "tableReference" "tableId"))
               (propertize 'annotation " [TABLE]"))
           data)))

(defun bq-find-used-tables ()
  (-let ((text (buffer-string)))
    (--map (seq-elt it 1)
           (s-match-strings-all "\\(?:join\\|from\\)\s+`?\\(\\(?:\\s_\\|\\w\\)+\\)"
                                text))))

(defun company-bq-query-backend (command &optional arg &rest _)
  (interactive (list 'interactive))
  (case command
    (interactive (company-begin-backend 'company-bq-query-backend))
    (prefix (and (eq major-mode 'bq-query-mode)
                 (if (bq--table-context-p)
                     (propertize (company-grab-symbol)
                                 'context 'table)
                   (propertize (company-grab-symbol)
                               'context 'field))))
    (candidates
     (progn
       (message "prefix => %s %s"  arg (get-text-property 0 'context arg))
       (remove-if-not
        (lambda (c) (string-prefix-p (downcase arg) c))
        (if (eql 'table (get-text-property 0 'context arg))
            (if (s-contains? "." arg)
                (bq-query-completions-tables-candidates (bq-extract-dataset arg))
              (bq-query-completions-datasets-candidates))
          (-let ((tables (bq-find-used-tables)))
            (message "used tables: %s" tables)
            (bq-query-completions-fields-candidates tables))))))
    (annotation (get-text-property 0 'annotation arg))
    (meta (get-text-property 0 'meta arg))
    (ignore-case t)))

(add-to-list 'company-backends 'company-bq-query-backend)

(provide 'bq-mode)
;;; bq-mode ends here
