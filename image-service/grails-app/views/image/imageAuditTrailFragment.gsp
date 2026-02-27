<ul>
<g:each in="${messages}" var="message">
    <li>
        ${message.dateCreated}&nbsp;${message.userId}
        <div>
        ${message.message}
        </div>
    </li>
</g:each>
</ul>