syntax on
set hidden
set title
set ruler
set hlsearch
set incsearch
set expandtab
set smartindent
set shiftwidth=2
set tabstop=4
set softtabstop=2
set t_Co=256
set splitbelow
map <C-n> :bn<CR>
map <C-p> :bp<CR>

" 4-space tab widths for python (and pyrex)
autocmd FileType py* setlocal shiftwidth=4 tabstop=8 softtabstop=4

" # key toggle comments in python
function! TogglePythonComments()
 if match(getline("."), '^ *#') >= 0
   execute ':s+#++' |
 else
   execute ':s+^+#+' |
 endif
endfunction
autocmd FileType python map # :call TogglePythonComments()<cr>

function! GoFormat()
  if bufexists("godebug") != 0
      execute ":bw godebug"
  endif
  let l = line(".")
  let c = col(".")
  let tmpname = tempname()
  call writefile(getline(1,'$'), tmpname)
  let out = system("goimports " . tmpname)
  if v:shell_error == 0
      execute ":%!goimports " . tmpname . "| gofmt -s"
      call cursor(l, c)
      execute ":redraw!"
  else
      execute ":new | :f godebug | :setlocal buftype=nofile | :resize 10 | :put=out"
  endif
endfunction
autocmd BufWritePre *.go :call GoFormat()

" highlight lines over 79 cols, spaces at the end of lines and tab characters
highlight BadStyle ctermbg=darkred ctermfg=darkgray
autocmd FileType python match BadStyle "\(\%>79v.\+\|\t\| \+$\)"
autocmd FileType go match BadStyle "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"
autocmd FileType go set noet softtabstop=0 tabstop=4
autocmd FileType go map <C-t> :!go test<cr>
