;;; bq-mode.el --- Major mode for Google Big Query

;; Copyright (C) 2020 Nils Grunwald

;; Author: GRUNWALD Nils
;; URL: http://github.com/ngrunwald/ob-bigquery
;; Version: 0.1.0
;; Package-Requires: ((spinner "1.7.3") (aio "") (om "")
;;                    (ht "") (treepy ""))

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
(require 'treepy)

(setq lexical-binding t)

(aio-defun bq-get-datasets-list ()
  "Get the list of datasets in current project."
  (-let* ((p-result (bq-async-json-command "bq --format=json ls --max_results=10000")))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(aio-defun bq-get-tables-list (dataset)
  "Get the list of tables in given dataset."
  (-let* ((p-result (bq-async-json-command (format "bq --format=json ls --max_results=10000 %s" dataset))))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(aio-defun bq-get-entity-info (id)
  (-let* ((p-result (bq-async-json-command (format "bq --format=json show %s" id))))
    (aio-await p-result)
    (funcall (aio-result p-result))))

(defun bq--analyze-field (f-spec )
  (-if-let (fields (ht-get f-spec "fields"))
      '()
    (list (ht-set f-spec "path" '()))))

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

(setq bq-info-keys
      (list
       `(("id") . ("Id" . identity))
       `(("type") . ("Type" . identity))
       `(("timePartitioning" "type") . ("Time Partitioning Type" . identity))
       `(("view" "useLegacySql") . ("Use Legacy SQL" . identity))
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
    (org-mode)
    (spinner-start 'progress-bar)
    (aio-await data-p)
    (setq ng-data (funcall (aio-result data-p)))
    (condition-case err
        (-let* ((data (funcall (aio-result data-p)))
                ;; _  (setq ng-data data)
                (rows (cl-loop for elt in bq-info-keys
                               when (condition-case _
                                        (apply 'ht-get* data (car elt))
                                      (error nil))
                               collect
                               (-let [(ks . (label . f)) elt]
                                 (list label (funcall f (apply 'ht-get* data ks)))))))

          (-when-let (desc (ht-get data "description"))
            (insert "*Description*\n\n")
            (insert desc)
            (insert "\n\n"))

          (-when-let (query (condition-case _
                                (ht-get* data "view" "query")
                              (error nil)))
            (insert "*View Query*\n\n")
            (insert (-> (om-build-src-block :value query :language "sql") (om-to-trimmed-string)))
            (insert "\n\n"))

          (->> (apply 'om-build-table! rows)
               (om-to-trimmed-string)
               (insert))

          (spinner-stop))
      (error (message "ERROR => %s" err)))
    ))

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

(defun bq-async-json-command (cmd)
  "Get entity info as pretty text."
  (-let* ((temp-buffer "*BigQuery-Output*")
          (proc-name "*BigQuery-Output-Proc*")
          (promise (aio-promise))
          (proc (start-process-shell-command proc-name temp-buffer cmd)))
        (set-process-sentinel proc
                              (lambda (process event)
                                (save-excursion
                                  (switch-to-buffer temp-buffer)
                                  (-let* ((result-str (buffer-string))
                                          (json-object-type 'hash-table)
                                          (result (json-read-from-string result-str)))
                                    (aio-resolve promise (lambda () result))
                                    (kill-buffer)))))
        promise))

(aio-defun bq-datasets ()
  (interactive)
  (switch-to-buffer "*BigQuery-Datasets*")
  (setq bq-current-datasets-list '())
  (bq-datasets-mode)
  (spinner-start 'progress-bar)
  (-let ((p-datasets (bq-get-datasets-list)))
    (aio-await p-datasets)
    (setq bq-current-datasets-list (funcall (aio-result p-datasets)))
    (spinner-stop)
    (bq-datasets-mode)))

(aio-defun bq-tables-from-list ()
  (interactive)
  (-let ((id (tabulated-list-get-id)))
    (switch-to-buffer "*BigQuery-Tables*")
    (setq bq-current-tables-list '())
    (bq-tables-mode)
    (spinner-start 'progress-bar)
    (-let ((p-tables (bq-get-tables-list id)))
      (aio-await p-tables)
      (setq bq-current-tables-list (funcall (aio-result p-tables)))
      (spinner-stop)
      (bq-tables-mode))))

(defun bq-entity-info-from-list ()
  ""
  (interactive)
  (-let ((id (tabulated-list-get-id)))
    (bq-entity-info id)))

(defun bq-send-region (start end)
  (interactive)
  (-let* ((query (buffer-substring-no-properties start end))
          (proc (start-process-shell-command
                 "*bq-query*" "*BigQuery-Results*"
                 (format "bq --quiet --format=pretty query '%s'" query))))
    (switch-to-buffer "*BigQuery-Results*")
    (spinner-start 'progress-bar)
    (set-process-sentinel proc (lambda (process event) (spinner-stop)))))

(defun bq-send-paragraph ()
  "Send the current paragraph to Big Query"
  (interactive)
  (let ((start (save-excursion
                 (backward-paragraph)
                 (point)))
        (end (save-excursion
               (forward-paragraph)
               (point))))
    (bq-send-region start end)))

(provide 'bq-mode)
;;; bigquery-mode ends here
